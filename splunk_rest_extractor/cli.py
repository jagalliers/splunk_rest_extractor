"""splunk-extract: plan | run | validate | status | head | compact"""
from __future__ import annotations

import argparse
import dataclasses
import gzip
import hashlib
import logging
import os
import shutil
import signal
import sys
import time
import uuid
from pathlib import Path

import httpx

from . import envfile
from . import spl as splmod
from .client import SplunkClient, SplunkError
from .executor import Executor, RunConfig, config_dict
from .planner import Planner
from .state import State
from .timerange import get_tz, resolve_time
from .validate import LEVELS, validate

log = logging.getLogger("splunk_rest_extractor")

DEFAULT_FIELDS = "_time,_raw,index,sourcetype,source,host,_indextime,_bkt,_cd"


def _env(*names: str) -> str | None:
    for n in names:
        v = os.environ.get(n)
        if v:
            return v
    return None


def add_conn_args(p: argparse.ArgumentParser) -> None:
    g = p.add_argument_group("connection", "flags win over the environment, which wins over the .env file")
    g.add_argument("--url", help="management URL (env SPLUNK_URL; default https://127.0.0.1:8089)")
    g.add_argument("--token", help="bearer token (env SPLUNK_TOKEN); preferred")
    g.add_argument("--username", help="env SPLUNK_USERNAME")
    g.add_argument("--password", help="env SPLUNK_PASSWORD")
    g.add_argument("--ca-bundle", help="CA bundle for TLS verification (env SPLUNK_CA_BUNDLE)")
    g.add_argument("--env-file", metavar="PATH", help="KEY=VALUE file to load first (default: .env in the current directory, if present)")
    g.add_argument("--insecure", action="store_true", help="disable TLS verification (lab only)")
    g.add_argument("--read-timeout", type=float, default=900.0)


def resolve_connection(a: argparse.Namespace) -> None:
    """Fill connection settings from the environment, after loading the .env file. Flags already set win."""
    if a.env_file:
        path = Path(a.env_file)
        if not path.is_file():
            raise SystemExit(f"--env-file {path}: no such file")
    else:
        path = Path(".env")
    loaded = envfile.load(path)
    if loaded:
        log.info("loaded %s from %s", ", ".join(sorted(loaded)), path)
    a.url = a.url or _env("SPLUNK_URL") or "https://127.0.0.1:8089"
    a.token = a.token or _env("SPLUNK_TOKEN")
    a.username = a.username or _env("SPLUNK_USERNAME", "SPLUNK_ADMIN_USER")
    a.password = a.password or _env("SPLUNK_PASSWORD", "SPLUNK_ADMIN_PASS")
    a.ca_bundle = a.ca_bundle or _env("SPLUNK_CA_BUNDLE")


def make_client(a: argparse.Namespace) -> SplunkClient:
    resolve_connection(a)
    verify: bool | str = True
    if a.insecure:
        verify = False
    elif a.ca_bundle:
        verify = a.ca_bundle
    return SplunkClient(a.url, token=a.token, username=a.username, password=a.password, verify=verify,
                        read_timeout=a.read_timeout)


def add_search_args(p: argparse.ArgumentParser) -> None:
    g = p.add_argument_group("search")
    g.add_argument("--spl", help="the search (no earliest/latest inside it)")
    g.add_argument("--spl-file", help="read the search from a file")
    g.add_argument("--earliest", required=True, help="epoch, ISO-8601 (naive=UTC), or Splunk relative (e.g. -7d@d)")
    g.add_argument("--latest", required=True)
    g.add_argument("--no-pin", action="store_true", help="do not pin index_latest to run start")
    g.add_argument("--search-level", default="fast", choices=["fast", "smart", "verbose"])
    g.add_argument("--allow-warnings", action="store_true", help="treat WARN job messages as non-fatal (default strict)")
    g = p.add_argument_group("chunking")
    g.add_argument("--chunk-target-events", type=int, default=250_000)
    g.add_argument("--max-count", type=int, help="job max_count (default 2 x chunk target)")
    g.add_argument("--no-histogram", action="store_true", help="skip the planning histogram; use fixed --span chunks")
    g.add_argument("--span", type=int, default=3600, help="histogram bin span in seconds, or fixed chunk span with --no-histogram")
    g.add_argument("--min-span", type=int, default=1, help="smallest chunk span in seconds")
    g.add_argument("--tz", default="UTC", help="time zone for day directories")


def add_run_args(p: argparse.ArgumentParser) -> None:
    add_search_args(p)
    g = p.add_argument_group("output")
    g.add_argument("--out", required=True, help="output directory (one run per directory; re-run to resume)")
    g.add_argument("--fields", default=None,
                   help=f"comma list, or 'all' for every field Splunk returns (default: {DEFAULT_FIELDS}; "
                        "'all' when the SPL is transforming)")
    g.add_argument("--format", dest="fmt", default="ndjson", choices=["ndjson", "raw"])
    g.add_argument("--on-bad-utf8", default="replace", choices=["replace", "fail"])
    g.add_argument("--time-format", default="%Y-%m-%dT%H:%M:%S.%3N%:z", help="strftime for _time (Splunk output_time_format)")
    g = p.add_argument_group("execution")
    g.add_argument("--workers", type=int, default=2)
    g.add_argument("--mode", default="job", choices=["job", "export"])
    g.add_argument("--page-size", type=int, help="rows per results call (default: restapi.maxresultrows)")
    g.add_argument("--ttl", type=int, default=3600)
    g.add_argument("--max-attempts", type=int, default=3)
    g.add_argument("--newest-first", action="store_true")
    g.add_argument("--validate", default="plan", choices=LEVELS, help="validation level at end of run")
    g.add_argument("--sample", type=int, default=0, help="chunks to re-extract and compare at level full")


def acquire_lock(path: Path):
    """Exclusive, non-blocking lock on a file; portable across POSIX and Windows."""
    fh = open(path, "w")
    try:
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as e:
        fh.close()
        raise SystemExit(f"another splunk-extract run is active in {path.parent}") from e
    return fh


def setup_logging(out: Path | None, verbose: bool) -> None:
    fmt = "%(asctime)s %(levelname)-7s %(threadName)-4s %(message)s"
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stderr)]
    if out is not None:
        out.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(out / "run.log", encoding="utf-8"))
    logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO, format=fmt, handlers=handlers)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


def load_spl(a: argparse.Namespace) -> str:
    if a.spl_file:
        raw = Path(a.spl_file).read_text(encoding="utf-8")
    elif a.spl:
        raw = a.spl
    else:
        raise SystemExit("one of --spl / --spl-file is required")
    spl = splmod.normalize(raw)
    issues = splmod.validate(spl)
    for i in issues:
        (log.error if i.level == "error" else log.warning)("SPL %s: %s", i.level, i.text)
    if any(i.level == "error" for i in issues):
        raise SystemExit(2)
    return spl


def resolve_range(client: SplunkClient, a: argparse.Namespace) -> tuple[int, int]:
    earliest = resolve_time(a.earliest, client.timeparser, round_up=False)
    latest = resolve_time(a.latest, client.timeparser, round_up=True)
    if latest <= earliest:
        raise SystemExit(f"latest ({latest}) must be after earliest ({earliest})")
    return earliest, latest


def build_planner(client: SplunkClient, spl: str, pin: int | None, a: argparse.Namespace, page_size: int) -> Planner:
    return Planner(client, spl, pin=pin, target=a.chunk_target_events, min_span=a.min_span, tz=get_tz(a.tz),
                   search_level=a.search_level, page_size=page_size)


def print_plan(chunks) -> None:
    print(f"{'day':10s} {'start':>11s} {'end':>11s} {'span_s':>8s} {'expected':>10s} hot")
    for c in chunks:
        print(f"{c.day:10s} {c.start:>11d} {c.end:>11d} {c.end - c.start:>8d} {str(c.expected) if c.expected is not None else '?':>10s} {'*' if c.hot else ''}")
    known = [c.expected for c in chunks if c.expected is not None]
    print(f"{len(chunks)} chunks, expected total {sum(known)} ({len(known)} with known counts)")


# ------------------------------------------------------------------ commands
def cmd_plan(a: argparse.Namespace) -> int:
    setup_logging(None, a.verbose)
    spl = load_spl(a)
    client = make_client(a)
    earliest, latest = resolve_range(client, a)
    pin = None if a.no_pin else int(time.time())
    limits = client.limits()
    log.info("range [%d,%d) pin=%s limits=%s", earliest, latest, pin, limits)
    planner = build_planner(client, spl, pin, a, limits["maxresultrows"])
    chunks = planner.plan(earliest, latest, use_histogram=not a.no_histogram, span=a.span)
    print_plan(chunks)
    return 0


def cmd_run(a: argparse.Namespace) -> int:
    out = Path(a.out)
    setup_logging(out, a.verbose)
    spl = load_spl(a)
    spl_sha = hashlib.sha256(spl.encode()).hexdigest()
    client = make_client(a)
    info = client.server_info()
    limits = client.limits()
    quotas = client.quotas()
    log.info("connected to %s (Splunk %s) as %s roles=%s", a.url, info.get("version"), quotas["username"], quotas["roles"])
    log.info("limits: maxresultrows=%s max_count=%s (readable=%s); quotas: jobs=%s disk=%sMB",
             limits["maxresultrows"], limits["max_count"], limits["readable"], quotas["srchJobsQuota"], quotas["srchDiskQuota"])

    if a.fields is None:
        if splmod.is_whole_set(spl):
            log.warning("SPL contains a transforming/whole-set command; keeping every field Splunk returns (pass --fields to override)")
            fields = None
        else:
            fields = DEFAULT_FIELDS.split(",")
    elif a.fields.strip().lower() == "all":
        fields = None
    else:
        fields = [f.strip() for f in a.fields.split(",") if f.strip()]
    if fields and "_raw" not in fields and a.fmt == "raw":
        fields.append("_raw")
    cfg = RunConfig(
        workers=a.workers, mode=a.mode, chunk_target=a.chunk_target_events,
        max_count=a.max_count or 2 * a.chunk_target_events,
        page_size=a.page_size or limits["maxresultrows"], min_span=a.min_span, ttl=a.ttl,
        search_level=a.search_level, strict=not a.allow_warnings, fields=fields, fmt=a.fmt,
        max_attempts=a.max_attempts, oldest_first=not a.newest_first, on_bad_utf8=a.on_bad_utf8,
        time_format=a.time_format,
    )
    if cfg.page_size > limits["maxresultrows"]:
        log.warning("--page-size %d exceeds restapi.maxresultrows %d; Splunk will cap it", cfg.page_size, limits["maxresultrows"])
    if quotas["srchDiskQuota"]:
        est_mb = cfg.workers * cfg.max_count * 700 / 1e6
        if est_mb > quotas["srchDiskQuota"] * 0.8:
            log.warning("estimated dispatch disk %.0fMB (workers x max_count x ~700B) is near srchDiskQuota %dMB; "
                        "lower --chunk-target-events/--workers or use --mode export", est_mb, quotas["srchDiskQuota"])
    if quotas["srchJobsQuota"] and cfg.workers + 1 > quotas["srchJobsQuota"]:
        log.warning("--workers %d exceeds srchJobsQuota %d", cfg.workers, quotas["srchJobsQuota"])

    lock = acquire_lock(out / ".lock")  # noqa: F841 - held for the life of the process
    state = State(out / "manifest.sqlite")
    run = state.get_run()
    if run is None:
        earliest, latest = resolve_range(client, a)
        pin = None if a.no_pin else int(time.time())
        run_id = uuid.uuid4().hex[:12]
        planner = build_planner(client, spl, pin, a, cfg.page_size)
        log.info("planning [%d,%d) pin=%s histogram=%s", earliest, latest, pin, not a.no_histogram)
        chunks = planner.plan(earliest, latest, use_histogram=not a.no_histogram, span=a.span)
        opts = config_dict(cfg) | {"tz": a.tz, "histogram": not a.no_histogram, "span": a.span, "validate": a.validate}
        # A planner-hot interval goes straight to export only when a job could not hold it; otherwise job mode
        # (with its error channel) is used and the overflow path bisects if the estimate was wrong.
        specs = [(c.day, c.start, c.end, c.expected, c.hot,
                  "export" if (cfg.mode == "export" or (c.hot and (c.expected or 0) > cfg.max_count)) else "job", None)
                 for c in chunks]
        state.create_run_with_chunks(run_id, spl, spl_sha, earliest, latest, pin, opts,
                                     {"version": info.get("version"), "serverName": info.get("serverName"), "url": a.url}, specs)
        known = [c.expected for c in chunks if c.expected is not None]
        log.info("plan: %d chunks, expected total %d (%d with known counts), %d hot", len(chunks), sum(known), len(known), sum(c.hot for c in chunks))
        run = state.get_run()
    else:
        if run["spl_sha"] != spl_sha:
            raise SystemExit(f"{out} holds a run for a different SPL; use a new --out")
        earliest, latest = resolve_range(client, a)
        if (earliest, latest) != (run["earliest"], run["latest"]):
            log.warning("resuming with the manifest's range [%d,%d), ignoring --earliest/--latest", run["earliest"], run["latest"])
        # Everything except worker count and attempt budget comes from the manifest so output stays consistent.
        for f in dataclasses.fields(RunConfig):
            if f.name in ("workers", "max_attempts") or f.name not in run["options"]:
                continue
            stored = run["options"][f.name]
            if stored != getattr(cfg, f.name):
                log.warning("resuming with %s=%r from the manifest (command line said %r)", f.name, stored, getattr(cfg, f.name))
                setattr(cfg, f.name, stored)
        log.info("resuming run %s: %s", run["id"], state.counts())

    ex = Executor(client, state, cfg, out, spl, run["pin"])
    try:
        ex.run()
    except KeyboardInterrupt:
        state.finish_run("interrupted")
        log.warning("interrupted; re-run the same command to resume")
        return 130
    counts = state.counts()
    ok = not any(counts.get(s) for s in ("pending", "running", "failed", "mismatch"))
    try:
        report = validate(client, state, out, a.validate, sample=a.sample)
    except BaseException:
        state.finish_run("failed")  # never leave the manifest saying 'running' with no process behind it
        raise
    state.finish_run("done" if ok and report["ok"] else "failed")
    log.info("run %s finished: %s; validation(%s) %s; report at %s", run["id"], counts, a.validate,
             "OK" if report["ok"] else "FAILED", out / "report.md")
    return 0 if (ok and report["ok"]) else 1


def cmd_validate(a: argparse.Namespace) -> int:
    out = Path(a.out)
    setup_logging(out, a.verbose)
    state = State(out / "manifest.sqlite")
    if state.get_run() is None:
        raise SystemExit(f"no run in {out}")
    client = make_client(a) if a.level in ("total", "full") else None
    report = validate(client, state, out, a.level, sample=a.sample)
    print((out / "report.md").read_text(encoding="utf-8"))
    return 0 if report["ok"] else 1


def cmd_status(a: argparse.Namespace) -> int:
    setup_logging(None, a.verbose)
    out = Path(a.out)
    state = State(out / "manifest.sqlite")
    run = state.get_run()
    if run is None:
        raise SystemExit(f"no run in {out}")
    print(f"run {run['id']} status={run['status']} spl={run['spl']!r}")
    print(f"range [{run['earliest']},{run['latest']}) pin={run['pin']}")
    print("chunks:", state.counts())
    print("totals:", state.totals())
    for c in state.chunks():
        if c.status in ("failed", "mismatch", "running", "pending"):
            print(f"  #{c.id} {c.day} [{c.start},{c.end}) {c.status} mode={c.mode} attempts={c.attempts} expected={c.expected} written={c.written} err={c.error}")
    return 0


def cmd_head(a: argparse.Namespace) -> int:
    """Print the first N lines of the run's output without needing gzcat/zcat."""
    out = Path(a.out)
    state = State(out / "manifest.sqlite")
    if state.get_run() is None:
        raise SystemExit(f"no run in {out}")
    seen: set[str] = set()
    remaining = a.n
    for c in state.chunks("done"):
        if not c.path or c.path in seen or remaining <= 0:
            continue
        seen.add(c.path)
        with gzip.open(c.path, "rt", encoding="utf-8", errors="replace") as fh:
            for line in fh:
                if remaining <= 0:
                    break
                sys.stdout.write(line)
                remaining -= 1
    return 0


def cmd_compact(a: argparse.Namespace) -> int:
    """Concatenate each day's chunk files (gzip members) into one day file, driven by the manifest."""
    setup_logging(None, a.verbose)
    out = Path(a.out)
    data = out / "data"
    state = State(out / "manifest.sqlite")
    run = state.get_run()
    if run is None:
        raise SystemExit(f"no run in {out}")
    leaf = [c for c in state.chunks() if c.status != "split"]
    not_done = [c for c in leaf if c.status != "done"]
    if not_done:
        raise SystemExit(f"refusing to compact: {len(not_done)} chunk(s) are not done (run `status`)")
    ext = ".jsonl.gz" if run["options"].get("fmt", "ndjson") == "ndjson" else ".raw.gz"
    by_day: dict[str, list] = {}
    for c in leaf:
        by_day.setdefault(c.day, []).append(c)
    for day, group in sorted(by_day.items()):
        group.sort(key=lambda c: c.start)
        target = data / f"{day}{ext}"
        if all(c.path == str(target) for c in group):
            continue  # already compacted
        tmp = target.with_name(target.name + ".part")
        with open(tmp, "wb") as dst:
            for c in group:
                with open(c.path, "rb") as src:
                    shutil.copyfileobj(src, dst)
            dst.flush()
            os.fsync(dst.fileno())
        os.replace(tmp, target)
        with gzip.open(target, "rb") as fh:
            n = sum(1 for _ in fh)
        want = sum((c.written or 0) + ((c.multiline or 0) if ext == ".raw.gz" else 0) for c in group)
        if n != want:
            target.unlink()
            raise SystemExit(f"{target}: {n} lines but manifest expects {want}; compaction aborted")
        old_paths = [Path(c.path) for c in group]
        for c in group:
            state.update_chunk(c.id, path=str(target))
        print(f"{target}: {len(group)} chunk files, {n} lines")
        if a.delete_parts:
            for p in old_paths:
                p.unlink(missing_ok=True)
            for d in {p.parent for p in old_paths}:
                if d.exists() and not any(d.iterdir()):
                    d.rmdir()
    return 0


TIME_OPTS = ("--earliest", "--latest")


def join_time_values(argv: list[str]) -> list[str]:
    """Turn `--earliest -1d@d` into `--earliest=-1d@d`.

    argparse before Python 3.14 treats a value that starts with '-' as an unknown option and fails with
    "expected one argument", so Splunk relative times could only be passed in the `--opt=value` form.
    """
    out: list[str] = []
    it = iter(argv)
    for tok in it:
        if tok in TIME_OPTS:
            nxt = next(it, None)
            if nxt is None:
                out.append(tok)
                break
            out.append(f"{tok}={nxt}")
        else:
            out.append(tok)
    return out


def main(argv: list[str] | None = None) -> int:
    argv = join_time_values(sys.argv[1:] if argv is None else list(argv))
    # Event lines and the report carry arbitrary Unicode. On Windows a redirected stdout/stderr defaults to the
    # legacy code page (cp1252) and `head`/`validate` would die with UnicodeEncodeError; make them UTF-8 everywhere,
    # matching the files the tool writes.
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, ValueError):  # not a TextIOWrapper (e.g. replaced by a test harness)
            pass
    p = argparse.ArgumentParser(prog="splunk-extract", description=__doc__)
    p.add_argument("-v", "--verbose", action="store_true")
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("plan", help="dry run: show the chunk plan")
    add_conn_args(sp)
    add_search_args(sp)
    sp.set_defaults(fn=cmd_plan)

    sp = sub.add_parser("run", help="plan, extract, validate (re-run to resume)")
    add_conn_args(sp)
    add_run_args(sp)
    sp.set_defaults(fn=cmd_run)

    sp = sub.add_parser("validate", help="validate an existing run")
    add_conn_args(sp)
    sp.add_argument("--out", required=True)
    sp.add_argument("--level", default="full", choices=LEVELS)
    sp.add_argument("--sample", type=int, default=0)
    sp.set_defaults(fn=cmd_validate)

    sp = sub.add_parser("status", help="show run progress")
    sp.add_argument("--out", required=True)
    sp.set_defaults(fn=cmd_status)

    sp = sub.add_parser("head", help="print the first lines of a run's output")
    sp.add_argument("--out", required=True)
    sp.add_argument("-n", type=int, default=5)
    sp.set_defaults(fn=cmd_head)

    sp = sub.add_parser("compact", help="concatenate each day's chunk files into one file per day")
    sp.add_argument("--out", required=True)
    sp.add_argument("--delete-parts", action="store_true")
    sp.set_defaults(fn=cmd_compact)

    a = p.parse_args(argv)

    def _term(signum, frame):  # SIGTERM from a supervisor behaves like Ctrl-C: finish the current page, then stop
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, _term)
    try:
        return a.fn(a)
    except SplunkError as e:
        log.error("%s", e)
        return 1
    except httpx.HTTPError as e:  # connection refused, TLS failure, timeout: one line, not a traceback
        hint = " (self-signed certificate? use --ca-bundle or, for a lab, --insecure)" if "CERTIFICATE_VERIFY_FAILED" in str(e) else ""
        log.error("%s: %s%s", getattr(a, "url", "connection"), e or type(e).__name__, hint)
        return 1
    except Exception:  # noqa: BLE001 - record the traceback in run.log, not just on the terminal
        log.exception("unexpected error")
        return 1


if __name__ == "__main__":
    sys.exit(main())
