"""Executor state machine against a fake Splunk: paging, overflow/split, export fallback, retries, failures."""
from __future__ import annotations

import gzip
from pathlib import Path

from splunk_rest_extractor.client import JobStatus
from splunk_rest_extractor.executor import Executor, RunConfig
from splunk_rest_extractor.state import State
from splunk_rest_extractor.validate import coverage_gaps


class FakeClient:
    """Serves events from a list of (epoch, raw) through the client surface the executor uses."""

    def __init__(self, events: list[tuple[int, str]]):
        self.events = events
        self.jobs: dict[str, dict] = {}
        self.created = 0
        self.deleted: list[str] = []
        self.fail_windows: dict[tuple[int, int], list[dict[str, str]]] = {}
        self.export_drop_lastrow = False
        self.export_calls = 0

    def _rows(self, a: int, b: int) -> list[dict]:
        return [{"_time": str(t), "_raw": r} for t, r in self.events if a <= t < b]

    def create_job(self, spl, earliest, latest, *, index_latest=None, max_count=None, ttl=3600, search_level="fast"):
        self.created += 1
        sid = f"sid{self.created}"
        rows = self._rows(earliest, latest)
        truncated = max_count is not None and len(rows) > max_count
        self.jobs[sid] = {"rows": rows[:max_count] if truncated else rows, "truncated": truncated, "win": (earliest, latest)}
        return sid

    def wait_job(self, sid, *, stop=None, **_):
        j = self.jobs[sid]
        msgs = self.fail_windows.get(j["win"], [])
        failed = any(m["type"] == "FATAL" for m in msgs)
        n = len(j["rows"])
        return JobStatus(sid, {"dispatchState": "FAILED" if failed else "DONE", "isDone": True, "isFailed": failed,
                               "isFinalized": False, "eventCount": n, "resultCount": n, "scanCount": n,
                               "eventIsTruncated": j["truncated"], "messages": msgs})

    def results_page(self, sid, offset, count, *, time_format=None):
        return self.jobs[sid]["rows"][offset:offset + count], 0

    def delete_job(self, sid):
        self.deleted.append(sid)
        self.jobs.pop(sid, None)

    def export(self, spl, earliest, latest, *, index_latest=None, search_level="fast", time_format=None):
        self.export_calls += 1
        for i, r in enumerate(self._rows(earliest, latest)):
            yield {"preview": False, "offset": i, "result": r}, 0
        if not self.export_drop_lastrow:
            yield {"preview": False, "lastrow": True}, 0

    def run_scalar_search(self, spl, earliest, latest, **_):
        n = len(self._rows(earliest, latest))
        return [{"count": str(n)}], JobStatus("count", {"dispatchState": "DONE", "isDone": True, "resultCount": 1})


def setup(tmp_path: Path, events, chunks, **cfg_over):
    tmp_path.mkdir(parents=True, exist_ok=True)
    state = State(tmp_path / "manifest.sqlite")
    mode = cfg_over.get("mode", "job")
    state.create_run_with_chunks("r", "search x", "sha", chunks[0][1], chunks[-1][2], None, {}, {},
                                 [(d, s, e, x, False, mode, None) for d, s, e, x in chunks])
    cfg = RunConfig(workers=1, page_size=3, max_attempts=2, retry_delay=0, fields=["_time", "_raw"], **cfg_over)
    client = FakeClient(events)
    ex = Executor(client, state, cfg, tmp_path, "search x", None)
    return client, state, ex


def lines(path: str) -> list[str]:
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        return fh.read().splitlines()


def test_job_mode_pages_writes_and_deletes_job(tmp_path):
    events = [(t, f"e{t}") for t in range(0, 100, 10)]  # 10 events, page size 3 -> 4 pages
    client, state, ex = setup(tmp_path, events, [("d", 0, 100, 10)])
    ex.run()
    c = state.chunks()[0]
    assert c.status == "done" and c.written == 10 and c.result_count == 10 and c.pages_done == 4
    assert len(lines(c.path)) == 10 and '"_raw":"e0"' in lines(c.path)[0]
    assert client.deleted == ["sid1"] and not client.jobs
    assert not list(tmp_path.glob("data/*/*.part"))


def test_overflow_bisects_until_chunks_fit(tmp_path):
    events = [(t, f"e{t}") for t in range(0, 100, 10)]
    client, state, ex = setup(tmp_path, events, [("d", 0, 100, 10)], max_count=4)
    ex.run()
    chunks = state.chunks()
    leaf = [c for c in chunks if c.status != "split"]
    assert all(c.status == "done" for c in leaf) and any(c.status == "split" for c in chunks)
    assert coverage_gaps(leaf, 0, 100) == []
    assert sum(c.written for c in leaf) == 10
    assert all(c.written <= 4 for c in leaf)
    assert not client.jobs, "every job must be deleted, including overflowed ones"


def test_unsplittable_hot_second_falls_back_to_export(tmp_path):
    events = [(5, f"e{i}") for i in range(10)]  # ten events in one second
    client, state, ex = setup(tmp_path, events, [("d", 5, 6, None)], max_count=4)
    ex.run()
    c = state.chunks()[0]
    assert c.status == "done" and c.mode == "export" and c.written == 10 and c.expected == 10
    assert client.export_calls == 1 and not client.jobs


def test_export_stream_without_lastrow_fails_after_attempts(tmp_path):
    events = [(5, "e")]
    client, state, ex = setup(tmp_path, events, [("d", 5, 6, None)], mode="export")
    client.export_drop_lastrow = True
    ex.run()
    c = state.chunks()[0]
    assert c.status == "failed" and c.attempts == 2 and "lastrow" in c.error
    assert not list(tmp_path.glob("data/**/*.gz"))


def test_plan_mismatch_retries_then_records_mismatch(tmp_path):
    events = [(t, "e") for t in range(0, 100, 10)]
    client, state, ex = setup(tmp_path, events, [("d", 0, 100, 11)])  # planner said 11, Splunk has 10
    ex.run()
    c = state.chunks()[0]
    assert c.status == "mismatch" and c.attempts == 2 and c.written == 10
    assert client.created == 2


def test_job_failure_leaves_no_file_and_deletes_job(tmp_path):
    events = [(t, "e") for t in range(0, 100, 10)]
    client, state, ex = setup(tmp_path, events, [("d", 0, 100, 10)])
    client.fail_windows[(0, 100)] = [{"type": "FATAL", "text": "Error in 'lookup' command"}]
    ex.run()
    c = state.chunks()[0]
    assert c.status == "failed" and "FATAL" in c.error and c.attempts == 2
    assert not list(tmp_path.glob("data/**/*")) or not list(tmp_path.glob("data/**/*.gz"))
    assert not client.jobs


def test_strict_warning_fails_chunk_but_allow_warnings_passes(tmp_path):
    events = [(t, "e") for t in range(0, 100, 10)]
    warn = [{"type": "WARN", "text": "search peer x is down"}]
    client, state, ex = setup(tmp_path, events, [("d", 0, 100, 10)])
    client.fail_windows[(0, 100)] = warn
    ex.run()
    assert state.chunks()[0].status == "failed"
    client2, state2, ex2 = setup(tmp_path / "lenient", events, [("d", 0, 100, 10)], strict=False)
    client2.fail_windows[(0, 100)] = warn
    ex2.run()
    c = state2.chunks()[0]
    assert c.status == "done" and c.messages == warn
