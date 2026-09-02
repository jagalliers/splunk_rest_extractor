"""Validation levels: job (stored evidence), plan (expected vs written), total (one count), full (files + recounts)."""
from __future__ import annotations

import gzip
import hashlib
import json
import logging
import random
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

from .client import SplunkClient
from .spl import count_spl, with_fields
from .state import Chunk, State
from .writer import ChunkWriter, read_chunk_file

log = logging.getLogger(__name__)

LEVELS = ["job", "plan", "total", "full"]


def coverage_gaps(leaf: list[Chunk], earliest: int, latest: int) -> list[str]:
    """Reasons the leaf chunks do not tile [earliest, latest) exactly."""
    gaps: list[str] = []
    if not leaf:
        return ["no chunks in manifest"]
    s = sorted(leaf, key=lambda c: (c.start, c.end))
    if s[0].start != earliest:
        gaps.append(f"first chunk starts at {s[0].start}, run starts at {earliest}")
    if s[-1].end != latest:
        gaps.append(f"last chunk ends at {s[-1].end}, run ends at {latest}")
    for c in s:
        if c.end <= c.start:
            gaps.append(f"chunk {c.id} has empty/negative span [{c.start},{c.end})")
    for a, b in zip(s, s[1:]):
        if a.end < b.start:
            gaps.append(f"gap [{a.end},{b.start}) between chunks {a.id} and {b.id}")
        elif a.end > b.start:
            gaps.append(f"overlap [{b.start},{min(a.end, b.end)}) between chunks {a.id} and {b.id}")
    return gaps[:20]


def validate(client: SplunkClient | None, state: State, out_dir: Path, level: str, *, sample: int = 0) -> dict[str, Any]:
    run = state.get_run()
    assert run is not None
    opts = run["options"]
    strict = bool(opts.get("strict", True))
    search_level = opts.get("search_level", "fast")
    page_size = int(opts.get("page_size", 50000))
    lvl = LEVELS.index(level)
    report: dict[str, Any] = {"level": level, "run": run["id"], "ok": True, "checks": [], "chunks_with_issues": []}

    def check(name: str, ok: bool, detail: str) -> None:
        report["checks"].append({"name": name, "ok": ok, "detail": detail})
        if not ok:
            report["ok"] = False
        (log.info if ok else log.error)("check %-26s %s  %s", name, "OK " if ok else "FAIL", detail)

    def issue(c: Chunk, **extra: Any) -> None:
        report["chunks_with_issues"].append({"id": c.id, "day": c.day, "start": c.start, "end": c.end, "status": c.status, **extra})

    chunks = state.chunks()
    leaf = [c for c in chunks if c.status != "split"]
    done = [c for c in leaf if c.status == "done"]
    bad = [c for c in leaf if c.status != "done"]

    # ---- structural: the leaf chunks must tile the requested range exactly
    gaps = coverage_gaps(leaf, run["earliest"], run["latest"])
    check("coverage", not gaps, "leaf chunks tile the range exactly" if not gaps else "; ".join(gaps))

    check("all-chunks-done", not bad, f"{len(done)} done, {len(bad)} not done ({', '.join(sorted({c.status for c in bad})) or '-'})")
    for c in bad:
        issue(c, error=c.error)

    # ---- job level: the stored per-chunk job evidence
    job_bad = [c for c in done if c.mode == "job" and c.result_count is not None and c.written != c.result_count]
    check("written-eq-resultCount", not job_bad, f"{len(job_bad)} job-mode chunk(s) with written != resultCount")
    for c in job_bad:
        issue(c, written=c.written, result_count=c.result_count)
    errored = [c for c in done if any(m["type"] in ("ERROR", "FATAL") for m in c.messages)]
    warned = [c for c in done if any(m["type"] == "WARN" for m in c.messages)]
    check("no-job-warnings", not errored and not (strict and warned),
          f"{len(errored)} chunk(s) with ERROR/FATAL, {len(warned)} with WARN messages{'' if strict else ' (allowed by --allow-warnings)'}")
    utf8 = sum(c.utf8_replacements for c in done)
    report["utf8_replacements"] = utf8
    if utf8:
        log.warning("%d invalid UTF-8 sequence(s) were replaced with U+FFFD across %d chunk(s)", utf8,
                    sum(1 for c in done if c.utf8_replacements))

    # ---- plan level: histogram expectations, including split parents vs children
    if lvl >= 1:
        plan_bad = [c for c in done if c.expected is not None and c.written != c.expected]
        top_unknown = [c for c in done if c.expected is None and c.parent is None]
        vacuous = bool(opts.get("histogram")) and done and len(top_unknown) == len([c for c in done if c.parent is None])
        check("written-eq-expected", not plan_bad and not vacuous,
              f"{len(plan_bad)} chunk(s) differ from planned count; {len(top_unknown)} top-level chunk(s) had no planned count"
              + (" (histogram ran but produced no usable counts)" if vacuous else ""))
        for c in plan_bad:
            issue(c, written=c.written, expected=c.expected)
        split_bad = []
        for p in [c for c in chunks if c.status == "split" and c.expected is not None]:
            total = _descendant_written(state, p.id)
            if total != p.expected:
                split_bad.append((p, total))
        check("split-parents-eq-children", not split_bad, f"{len(split_bad)} split chunk(s) whose children do not add up")
        for p, total in split_bad:
            issue(p, expected=p.expected, children_written=total)

    written_total = sum(c.written or 0 for c in done)
    report["written_total"] = written_total

    # ---- total level: one independent count over the whole range
    if lvl >= 2 and client is not None:
        rows, st = client.run_scalar_search(
            count_spl(run["spl"]), run["earliest"], run["latest"], index_latest=run["pin"],
            search_level=search_level, page_size=page_size, strict=strict,
        )
        total = int(rows[0]["count"]) if rows else 0
        report["independent_total"] = total
        check("total-count", total == written_total and not bad,
              f"independent `| stats count` = {total}, written = {written_total}, scanCount = {st.scan_count}")

    # ---- full level: re-read every file, re-count every chunk, optional re-extract sample
    if lvl >= 3:
        file_bad: list[tuple[Chunk, str]] = []
        by_path: dict[str, list[Chunk]] = defaultdict(list)
        for c in done:
            if not c.path:
                file_bad.append((c, "no path recorded"))
            else:
                by_path[c.path].append(c)
        for path, group in by_path.items():
            p = Path(path)
            if not p.exists():
                file_bad.extend((c, "missing") for c in group)
                continue
            try:
                n, sha, _ = read_chunk_file(p)
            except (EOFError, OSError, gzip.BadGzipFile) as e:
                file_bad.extend((c, f"unreadable: {e!r}") for c in group)
                continue
            want_lines = sum((c.written or 0) + (c.multiline or 0 if opts.get("fmt") == "raw" else 0) for c in group)
            if n != want_lines:
                file_bad.extend((c, f"lines={n} expected={want_lines}") for c in group)
            elif len(group) == 1 and sha != group[0].sha256:
                file_bad.append((group[0], "sha256 mismatch"))
        check("files-intact", not file_bad, f"{len(file_bad)} chunk file(s) differ from manifest (missing / line count / sha256)")
        for c, why in file_bad:
            issue(c, path=c.path, file=why)
        if client is not None:
            recount_bad = []
            t0 = time.time()
            for c in done:
                rows, _ = client.run_scalar_search(
                    count_spl(run["spl"]), c.start, c.end, index_latest=run["pin"],
                    search_level=search_level, page_size=page_size, strict=strict,
                )
                n = int(rows[0]["count"]) if rows else 0
                if n != c.written:
                    recount_bad.append((c, n))
            check("per-chunk-recount", not recount_bad, f"{len(recount_bad)} of {len(done)} chunk(s) recount differently ({time.time() - t0:.0f}s)")
            for c, n in recount_bad:
                issue(c, written=c.written, recount=n)
            candidates = [c for c in done if c.path and len(by_path.get(c.path, [])) == 1]
            if sample > 0 and candidates:
                picked = random.sample(candidates, min(sample, len(candidates)))
                sample_bad = [c for c in picked if not _same_content(client, run, opts, c, out_dir)]
                check("sample-reextract", not sample_bad, f"{len(sample_bad)} of {len(picked)} re-extracted chunk(s) differ in content")
                for c in sample_bad:
                    issue(c, path=c.path, reextract="content differs")
            elif sample > 0:
                log.info("sample-reextract skipped: no per-chunk files (compacted?)")

    tmp = out_dir / "tmp"
    if tmp.exists() and not any(tmp.iterdir()):
        tmp.rmdir()
    report["generated"] = time.time()
    (out_dir / "report.json").write_text(json.dumps(report, indent=2, default=str))
    (out_dir / "report.md").write_text(_markdown(report, run, state))
    return report


def _descendant_written(state: State, parent_id: int) -> int:
    total = 0
    for ch in state.children(parent_id):
        if ch.status == "split":
            total += _descendant_written(state, ch.id)
        else:
            total += ch.written or 0
    return total


def _same_content(client: SplunkClient, run: dict, opts: dict, c: Chunk, out_dir: Path) -> bool:
    """Re-extract a chunk via export and compare sorted line hashes with the file on disk."""
    tmp = out_dir / "tmp" / f"reextract-{c.id}.gz"
    tmp.parent.mkdir(exist_ok=True)
    w = ChunkWriter(tmp, opts.get("fields"), opts.get("fmt", "ndjson"))
    saw_last = False
    try:
        for obj, _ in client.export(with_fields(run["spl"], opts.get("fields")), c.start, c.end,
                                    index_latest=run["pin"], search_level=opts.get("search_level", "fast"),
                                    time_format=opts.get("time_format")):
            if "result" in obj:
                w.write_rows([obj["result"]])
            if obj.get("lastrow"):
                saw_last = True
        w.commit()
    except BaseException:
        w.abort()
        raise
    if not saw_last:
        tmp.unlink(missing_ok=True)
        return False

    def sorted_hash(p: Path) -> str:
        with gzip.open(p, "rb") as fh:
            lines = sorted(fh)
        h = hashlib.sha256()
        for ln in lines:
            h.update(ln)
        return h.hexdigest()

    same = sorted_hash(tmp) == sorted_hash(Path(c.path))
    tmp.unlink(missing_ok=True)
    return same


def _markdown(report: dict, run: dict, state: State) -> str:
    counts = state.counts()
    lines = [f"# Extraction report — run {run['id']}", "",
             f"* SPL: `{run['spl']}`", f"* Range: [{run['earliest']}, {run['latest']}) epoch, pin index_latest={run['pin']}",
             f"* Validation level: **{report['level']}** — overall **{'OK' if report['ok'] else 'FAILED'}**",
             f"* Rows written: {report.get('written_total', 0)}" + (f", independent total: {report['independent_total']}" if 'independent_total' in report else ""),
             "* Chunks: " + ", ".join(f"{k}={v}" for k, v in sorted(counts.items())),
             f"* Invalid UTF-8 replacements: {report.get('utf8_replacements', 0)}", "", "## Checks", ""]
    for c in report["checks"]:
        lines.append(f"* {'✅' if c['ok'] else '❌'} `{c['name']}` — {c['detail']}")
    if report["chunks_with_issues"]:
        lines += ["", "## Chunks with issues", ""]
        for c in report["chunks_with_issues"]:
            lines.append(f"* {json.dumps(c, default=str)}")
    return "\n".join(lines) + "\n"
