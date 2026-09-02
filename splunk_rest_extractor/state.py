"""SQLite manifest: one run per output directory, chunks with a small state machine."""
from __future__ import annotations

import json
import sqlite3
import threading
import time
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

SCHEMA = """
CREATE TABLE IF NOT EXISTS run (
  id TEXT PRIMARY KEY, created REAL, spl TEXT, spl_sha TEXT, earliest INTEGER, latest INTEGER,
  pin INTEGER, options TEXT, server TEXT, status TEXT, finished REAL
);
CREATE TABLE IF NOT EXISTS chunk (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  day TEXT NOT NULL, start INTEGER NOT NULL, end INTEGER NOT NULL,
  mode TEXT NOT NULL DEFAULT 'job', status TEXT NOT NULL DEFAULT 'pending',
  expected INTEGER, hot INTEGER DEFAULT 0, parent INTEGER,
  sid TEXT, event_count INTEGER, result_count INTEGER, scan_count INTEGER,
  written INTEGER, pages_done INTEGER DEFAULT 0, bytes INTEGER, sha256 TEXT,
  utf8_replacements INTEGER DEFAULT 0, multiline INTEGER DEFAULT 0, messages TEXT,
  attempts INTEGER DEFAULT 0, error TEXT, started REAL, finished REAL, path TEXT,
  not_before REAL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS chunk_status ON chunk(status, start);
CREATE TABLE IF NOT EXISTS event (ts REAL, chunk INTEGER, kind TEXT, detail TEXT);
"""

TERMINAL = ("done", "failed", "split", "mismatch")


@dataclass
class Chunk:
    id: int
    day: str
    start: int
    end: int
    mode: str
    status: str
    expected: int | None
    hot: bool
    parent: int | None
    sid: str | None
    event_count: int | None
    result_count: int | None
    scan_count: int | None
    written: int | None
    pages_done: int
    bytes: int | None
    sha256: str | None
    utf8_replacements: int
    multiline: int
    messages: list[dict[str, str]]
    attempts: int
    error: str | None
    started: float | None
    finished: float | None
    path: str | None

    @classmethod
    def from_row(cls, r: sqlite3.Row) -> Chunk:
        return cls(
            id=r["id"], day=r["day"], start=r["start"], end=r["end"], mode=r["mode"], status=r["status"],
            expected=r["expected"], hot=bool(r["hot"]), parent=r["parent"], sid=r["sid"],
            event_count=r["event_count"], result_count=r["result_count"], scan_count=r["scan_count"],
            written=r["written"], pages_done=r["pages_done"] or 0, bytes=r["bytes"], sha256=r["sha256"],
            utf8_replacements=r["utf8_replacements"] or 0, multiline=r["multiline"] or 0,
            messages=json.loads(r["messages"]) if r["messages"] else [], attempts=r["attempts"] or 0,
            error=r["error"], started=r["started"], finished=r["finished"], path=r["path"],
        )

    @property
    def span(self) -> int:
        return self.end - self.start


class State:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.RLock()
        self._db = sqlite3.connect(str(path), check_same_thread=False, isolation_level=None)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=NORMAL")
        self._db.executescript(SCHEMA)

    # ------------------------------------------------------------------ run
    def get_run(self) -> dict[str, Any] | None:
        with self._lock:
            r = self._db.execute("SELECT * FROM run LIMIT 1").fetchone()
        if r is None:
            return None
        d = dict(r)
        d["options"] = json.loads(d["options"]) if d["options"] else {}
        d["server"] = json.loads(d["server"]) if d["server"] else {}
        return d

    def create_run(self, run_id: str, spl: str, spl_sha: str, earliest: int, latest: int, pin: int | None,
                   options: dict[str, Any], server: dict[str, Any]) -> None:
        with self._lock:
            self._db.execute(
                "INSERT INTO run (id, created, spl, spl_sha, earliest, latest, pin, options, server, status) "
                "VALUES (?,?,?,?,?,?,?,?,?,'running')",
                (run_id, time.time(), spl, spl_sha, earliest, latest, pin, json.dumps(options), json.dumps(server)),
            )

    def create_run_with_chunks(self, run_id: str, spl: str, spl_sha: str, earliest: int, latest: int, pin: int | None,
                               options: dict[str, Any], server: dict[str, Any],
                               specs: Iterable[tuple[str, int, int, int | None, bool, str, int | None]]) -> None:
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                self.create_run(run_id, spl, spl_sha, earliest, latest, pin, options, server)
                self.add_chunks(specs)
                self._db.execute("COMMIT")
            except BaseException:
                self._db.execute("ROLLBACK")
                raise

    def split_chunk(self, parent_id: int, children: list[tuple[str, int, int]], error: str) -> None:
        """Insert the children and mark the parent split in one transaction (idempotent on the parent)."""
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                existing = self._db.execute("SELECT COUNT(*) FROM chunk WHERE parent=?", (parent_id,)).fetchone()[0]
                if existing == 0:
                    self._db.executemany(
                        "INSERT INTO chunk (day, start, end, parent) VALUES (?,?,?,?)",
                        [(d, s, e, parent_id) for d, s, e in children],
                    )
                self._db.execute("UPDATE chunk SET status='split', sid=NULL, finished=?, error=? WHERE id=?",
                                 (time.time(), error, parent_id))
                self._db.execute("COMMIT")
            except BaseException:
                self._db.execute("ROLLBACK")
                raise

    def finish_run(self, status: str) -> None:
        with self._lock:
            self._db.execute("UPDATE run SET status=?, finished=?", (status, time.time()))

    # --------------------------------------------------------------- chunks
    def add_chunks(self, specs: Iterable[tuple[str, int, int, int | None, bool, str, int | None]]) -> None:
        """specs: (day, start, end, expected, hot, mode, parent)"""
        with self._lock:
            self._db.executemany(
                "INSERT INTO chunk (day, start, end, expected, hot, mode, parent) VALUES (?,?,?,?,?,?,?)",
                [(d, s, e, x, int(h), m, p) for d, s, e, x, h, m, p in specs],
            )

    def reset_for_resume(self, retry_failed: bool = True) -> dict[str, Any]:
        """Requeue interrupted (and optionally failed/mismatch) chunks. Returns sids and paths to clean up."""
        with self._lock:
            rows = self._db.execute("SELECT id, sid, path FROM chunk WHERE status='running'").fetchall()
            sids = [r["sid"] for r in rows if r["sid"]]
            self._db.execute("UPDATE chunk SET status='pending', sid=NULL, pages_done=0, "
                             "attempts=MAX(attempts-1,0) WHERE status='running'")
            n_failed = 0
            if retry_failed:
                cur = self._db.execute("UPDATE chunk SET status='pending', sid=NULL, pages_done=0, attempts=0, "
                                       "error=NULL WHERE status IN ('failed','mismatch')")
                n_failed = cur.rowcount
            return {"interrupted": len(rows), "failed": n_failed, "sids": sids}

    def claim_next(self, oldest_first: bool = True) -> Chunk | None:
        order = "start ASC" if oldest_first else "start DESC"
        with self._lock:
            r = self._db.execute(
                f"UPDATE chunk SET status='running', started=?, attempts=attempts+1, error=NULL "
                f"WHERE id = (SELECT id FROM chunk WHERE status='pending' AND not_before<=? ORDER BY {order} LIMIT 1) "
                f"RETURNING *",
                (time.time(), time.time()),
            ).fetchone()
        return Chunk.from_row(r) if r else None

    def update_chunk(self, chunk_id: int, **fields: Any) -> None:
        if "messages" in fields and not isinstance(fields["messages"], str):
            fields["messages"] = json.dumps(fields["messages"])
        cols = ", ".join(f"{k}=?" for k in fields)
        with self._lock:
            self._db.execute(f"UPDATE chunk SET {cols} WHERE id=?", (*fields.values(), chunk_id))

    def get_chunk(self, chunk_id: int) -> Chunk:
        with self._lock:
            r = self._db.execute("SELECT * FROM chunk WHERE id=?", (chunk_id,)).fetchone()
        return Chunk.from_row(r)

    def chunks(self, status: str | None = None) -> list[Chunk]:
        with self._lock:
            if status:
                rows = self._db.execute("SELECT * FROM chunk WHERE status=? ORDER BY start", (status,)).fetchall()
            else:
                rows = self._db.execute("SELECT * FROM chunk ORDER BY start").fetchall()
        return [Chunk.from_row(r) for r in rows]

    def children(self, parent_id: int) -> list[Chunk]:
        with self._lock:
            rows = self._db.execute("SELECT * FROM chunk WHERE parent=? ORDER BY start", (parent_id,)).fetchall()
        return [Chunk.from_row(r) for r in rows]

    def counts(self) -> dict[str, int]:
        with self._lock:
            rows = self._db.execute("SELECT status, COUNT(*) n FROM chunk GROUP BY status").fetchall()
        return {r["status"]: r["n"] for r in rows}

    def totals(self) -> dict[str, Any]:
        with self._lock:
            r = self._db.execute(
                "SELECT COALESCE(SUM(written),0) w, COALESCE(SUM(bytes),0) b, COALESCE(SUM(utf8_replacements),0) u, "
                "COALESCE(SUM(multiline),0) m FROM chunk WHERE status IN ('done','mismatch')"
            ).fetchone()
        return {"written": r["w"], "bytes": r["b"], "utf8_replacements": r["u"], "multiline": r["m"]}

    def pending_work(self) -> bool:
        c = self.counts()
        return bool(c.get("pending") or c.get("running"))

    def log(self, chunk_id: int | None, kind: str, detail: str) -> None:
        with self._lock:
            self._db.execute("INSERT INTO event VALUES (?,?,?,?)", (time.time(), chunk_id, kind, detail[:4000]))

    def events(self, limit: int = 200) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._db.execute("SELECT * FROM event ORDER BY ts DESC LIMIT ?", (limit,)).fetchall()
        return [dict(r) for r in rows]

    def close(self) -> None:
        with self._lock:
            self._db.close()
