"""Worker pool that drives each chunk through job mode (default) or export mode."""
from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from pathlib import Path

from .client import Interrupted, JobGone, SplunkClient, SplunkError
from .spl import count_spl, with_fields
from .state import Chunk, State
from .timerange import bisect
from .writer import ChunkWriter, chunk_path

log = logging.getLogger(__name__)


class ChunkFailed(Exception):
    pass


class ChunkOverflow(Exception):
    pass


@dataclass
class RunConfig:
    workers: int = 2
    mode: str = "job"              # job | export
    chunk_target: int = 250_000
    max_count: int = 500_000       # job max_count (2 x target by default)
    page_size: int = 50_000
    min_span: int = 1
    ttl: int = 3600
    search_level: str = "fast"
    strict: bool = True
    fields: list[str] | None = None
    fmt: str = "ndjson"
    max_attempts: int = 3
    retry_delay: int = 10          # seconds x attempt number before a failed chunk is retried
    oldest_first: bool = True
    on_bad_utf8: str = "replace"   # replace | fail
    time_format: str = "%Y-%m-%dT%H:%M:%S.%3N%:z"  # applied to _time on both endpoints so outputs are comparable


class Executor:
    def __init__(self, client: SplunkClient, state: State, cfg: RunConfig, out_dir: Path, spl: str, pin: int | None) -> None:
        self.client = client
        self.state = state
        self.cfg = cfg
        self.data_dir = out_dir / "data"
        self.spl = spl
        self.extract_spl = with_fields(spl, cfg.fields)
        self.pin = pin
        self._stop = threading.Event()
        self._rows_since = 0
        self._rows_lock = threading.Lock()

    # ------------------------------------------------------------- driving
    def run(self) -> None:
        reset = self.state.reset_for_resume(retry_failed=True)
        if reset["interrupted"] or reset["failed"]:
            log.info("requeued %d interrupted and %d failed/mismatched chunk(s)", reset["interrupted"], reset["failed"])
        for sid in reset["sids"]:
            self.client.delete_job(sid)
        for part in self.data_dir.glob("*/*.part"):
            log.info("removing leftover %s", part)
            part.unlink()
        t0 = time.time()
        with ThreadPoolExecutor(max_workers=self.cfg.workers, thread_name_prefix="w") as pool:
            futures = [pool.submit(self._worker) for _ in range(self.cfg.workers)]
            try:
                last = time.time()
                while any(not f.done() for f in futures):
                    time.sleep(1)
                    if time.time() - last >= 15:
                        self._progress(t0)
                        last = time.time()
            except KeyboardInterrupt:
                log.warning("interrupt: stopping workers after their current page")
                self._stop.set()
                raise
            for f in futures:
                f.result()
        self._progress(t0)

    def _progress(self, t0: float) -> None:
        c = self.state.counts()
        t = self.state.totals()
        el = max(1.0, time.time() - t0)
        log.info("progress: %s | rows=%d (%.0f rows/s) bytes=%.1fMB",
                 " ".join(f"{k}={v}" for k, v in sorted(c.items())), t["written"], t["written"] / el, t["bytes"] / 1e6)

    def _worker(self) -> None:
        idle = 0
        while not self._stop.is_set():
            try:
                chunk = self.state.claim_next(self.cfg.oldest_first)
                if chunk is None:
                    if not self.state.pending_work():
                        return
                    idle += 1
                    time.sleep(min(5, 0.5 * idle))
                    continue
            except Exception:  # noqa: BLE001 - e.g. transient sqlite I/O error
                log.exception("manifest access failed; retrying in 5s")
                time.sleep(5)
                continue
            idle = 0
            try:
                self._process(chunk)
            except Exception as e:  # noqa: BLE001 - a worker must never die
                log.exception("chunk %d [%d,%d) unexpected error", chunk.id, chunk.start, chunk.end)
                self._fail(chunk, f"unexpected: {e!r}")

    # --------------------------------------------------------------- chunk
    def _process(self, chunk: Chunk) -> None:
        log.info("chunk %d %s [%d,%d) span=%ds mode=%s attempt=%d expected=%s", chunk.id, chunk.day, chunk.start,
                 chunk.end, chunk.span, chunk.mode, chunk.attempts, chunk.expected)
        try:
            if chunk.mode == "export":
                info = self._export_chunk(chunk)
            else:
                info = self._job_chunk(chunk)
        except ChunkOverflow:
            self._split(chunk)
            return
        except Interrupted:
            self._fail(chunk, "interrupted")
            return
        except (ChunkFailed, SplunkError) as e:
            self._fail(chunk, str(e))
            return
        self._finish(chunk, info)

    def _finish(self, chunk: Chunk, info: dict) -> None:
        fresh = self.state.get_chunk(chunk.id)
        expected = fresh.expected
        status = "done"
        err = None
        if expected is not None and info["written"] != expected:
            err = f"written {info['written']} != expected {expected} (from planning histogram)"
            if fresh.attempts < self.cfg.max_attempts:
                log.warning("chunk %d %s; re-running", chunk.id, err)
                Path(info["path"]).unlink(missing_ok=True)  # never leave a file that the manifest does not vouch for
                self.state.update_chunk(chunk.id, status="pending", error=err, sid=None, pages_done=0,
                                        written=None, bytes=None, sha256=None, path=None)
                self.state.log(chunk.id, "mismatch-retry", err)
                return
            status = "mismatch"
            log.error("chunk %d %s after %d attempts", chunk.id, err, fresh.attempts)
        self.state.update_chunk(
            chunk.id, status=status, error=err, written=info["written"], bytes=info["bytes"], sha256=info["sha256"],
            multiline=info["multiline"], path=info["path"], finished=time.time(),
        )
        self.state.log(chunk.id, status, f"written={info['written']} bytes={info['bytes']}")
        log.info("chunk %d %s: %d rows, %.1f MB", chunk.id, status, info["written"], info["bytes"] / 1e6)

    def _fail(self, chunk: Chunk, error: str) -> None:
        fresh = self.state.get_chunk(chunk.id)
        if fresh.sid:
            self.client.delete_job(fresh.sid)
        if error == "interrupted":
            self.state.update_chunk(chunk.id, status="pending", error=error, sid=None, pages_done=0, attempts=fresh.attempts - 1)
            self.state.log(chunk.id, "interrupted", "")
            return
        if fresh.attempts < self.cfg.max_attempts:
            delay = self.cfg.retry_delay * fresh.attempts
            log.warning("chunk %d failed (attempt %d): %s; retrying in %ds", chunk.id, fresh.attempts, error, delay)
            self.state.update_chunk(chunk.id, status="pending", error=error, sid=None, pages_done=0,
                                    not_before=time.time() + delay)
        else:
            log.error("chunk %d failed permanently: %s", chunk.id, error)
            chunk_path(self.data_dir, chunk.day, chunk.start, chunk.end, self.cfg.fmt).unlink(missing_ok=True)
            self.state.update_chunk(chunk.id, status="failed", error=error, sid=None, finished=time.time(),
                                    written=None, bytes=None, sha256=None, path=None)
        self.state.log(chunk.id, "failed", error)

    def _split(self, chunk: Chunk) -> None:
        fresh = self.state.get_chunk(chunk.id)
        if fresh.sid:
            self.client.delete_job(fresh.sid)
        if chunk.span > self.cfg.min_span and chunk.span >= 2:
            (a0, a1), (b0, b1) = bisect(chunk.start, chunk.end)
            self.state.split_chunk(chunk.id, [(chunk.day, a0, a1), (chunk.day, b0, b1)],
                                   "eventIsTruncated: split into two halves")
            self.state.log(chunk.id, "split", f"[{a0},{a1}) [{b0},{b1})")
            log.warning("chunk %d [%d,%d) overflowed max_count=%d; split", chunk.id, chunk.start, chunk.end, self.cfg.max_count)
        else:
            self.state.update_chunk(chunk.id, status="pending", mode="export", hot=1, sid=None, pages_done=0,
                                    attempts=0, error="eventIsTruncated at min span; switched to export mode")
            self.state.log(chunk.id, "export-fallback", "unsplittable hot interval")
            log.warning("chunk %d [%d,%d) overflows and cannot be split; switching to export mode", chunk.id, chunk.start, chunk.end)

    # ------------------------------------------------------------ job mode
    def _job_chunk(self, chunk: Chunk) -> dict:
        sid = self.client.create_job(
            self.extract_spl, chunk.start, chunk.end, index_latest=self.pin, max_count=self.cfg.max_count,
            ttl=self.cfg.ttl, search_level=self.cfg.search_level,
        )
        self.state.update_chunk(chunk.id, sid=sid)
        st = self.client.wait_job(sid, stop=self._stop)
        self.state.update_chunk(chunk.id, event_count=st.event_count, result_count=st.result_count,
                                scan_count=st.scan_count, messages=st.messages)
        if st.is_truncated:
            raise ChunkOverflow()
        probs = st.problems(strict=self.cfg.strict)
        if probs:
            raise ChunkFailed("; ".join(probs))

        path = chunk_path(self.data_dir, chunk.day, chunk.start, chunk.end, self.cfg.fmt)
        writer = ChunkWriter(path, self.cfg.fields, self.cfg.fmt)
        reps_total = 0
        try:
            offset = 0
            pages = 0
            while offset < st.result_count and not self._stop.is_set():
                try:
                    rows, reps = self.client.results_page(sid, offset, self.cfg.page_size, time_format=self.cfg.time_format)
                except JobGone as e:
                    raise ChunkFailed(f"job {sid} disappeared while paging at offset {offset}") from e
                if not rows:
                    break
                if reps and self.cfg.on_bad_utf8 == "fail":
                    raise ChunkFailed(f"{reps} invalid UTF-8 sequence(s) in page at offset {offset}")
                reps_total += reps
                writer.write_rows(rows)
                offset += len(rows)
                pages += 1
                self.state.update_chunk(chunk.id, pages_done=pages, utf8_replacements=reps_total)
            if self._stop.is_set():
                raise ChunkFailed("interrupted")
            if offset != st.result_count:
                raise ChunkFailed(f"paged {offset} rows but job resultCount is {st.result_count}")
            info = writer.commit()
        except BaseException:
            writer.abort()
            raise
        finally:
            self.client.delete_job(sid)
            self.state.update_chunk(chunk.id, sid=None)
        return info

    # --------------------------------------------------------- export mode
    def _export_chunk(self, chunk: Chunk) -> dict:
        path = chunk_path(self.data_dir, chunk.day, chunk.start, chunk.end, self.cfg.fmt)
        writer = ChunkWriter(path, self.cfg.fields, self.cfg.fmt)
        saw_last = False
        reps_total = 0
        try:
            for obj, reps in self.client.export(self.extract_spl, chunk.start, chunk.end,
                                                index_latest=self.pin, search_level=self.cfg.search_level,
                                                time_format=self.cfg.time_format):
                if reps and self.cfg.on_bad_utf8 == "fail":
                    raise ChunkFailed(f"{reps} invalid UTF-8 sequence(s) in export stream")
                reps_total += reps
                if obj.get("preview"):
                    continue
                if "result" in obj:
                    writer.write_rows([obj["result"]])
                if obj.get("lastrow"):
                    saw_last = True
                if self._stop.is_set():
                    raise ChunkFailed("interrupted")
            if not saw_last:
                raise ChunkFailed("export stream ended without a lastrow marker (search likely failed server-side)")
            info = writer.commit()
        except BaseException:
            writer.abort()
            raise
        self.state.update_chunk(chunk.id, utf8_replacements=reps_total)
        fresh = self.state.get_chunk(chunk.id)
        if fresh.expected is None:
            # Export has no job state to check against, so an independent count is mandatory.
            rows, _ = self.client.run_scalar_search(
                count_spl(self.spl), chunk.start, chunk.end, index_latest=self.pin,
                search_level=self.cfg.search_level, page_size=self.cfg.page_size, strict=self.cfg.strict,
                stop=self._stop,
            )
            expected = int(rows[0]["count"]) if rows else 0
            self.state.update_chunk(chunk.id, expected=expected)
        return info


def config_dict(cfg: RunConfig) -> dict:
    return asdict(cfg)
