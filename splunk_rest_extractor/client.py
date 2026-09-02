"""Thin Splunk REST client: auth, search jobs, results paging, export streaming.

Only the handful of endpoints the extractor needs. Every request goes through
``request()`` which handles retries, back-off, and re-login. Bodies are decoded
tolerantly because Splunk passes through raw bytes that are not valid UTF-8.
"""
from __future__ import annotations

import json
import logging
import random
import threading
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import httpx

log = logging.getLogger(__name__)

JOBS = "/services/search/v2/jobs"


class SplunkError(Exception):
    """Non-retryable error from Splunk or the transport."""


class AuthError(SplunkError):
    pass


class JobGone(SplunkError):
    """The job no longer exists on the server (404)."""


class RetryExhausted(SplunkError):
    pass


class Interrupted(Exception):
    """A stop was requested while waiting on Splunk."""


def decode_tolerant(data: bytes) -> tuple[str, int]:
    """Decode UTF-8, replacing invalid bytes; return (text, replacement_count)."""
    try:
        return data.decode("utf-8"), 0
    except UnicodeDecodeError:
        text = data.decode("utf-8", errors="replace")
        # Count only what the decoder inserted, not U+FFFD characters already present in the data.
        return text, len(text) - len(data.decode("utf-8", errors="ignore"))


@dataclass
class JobStatus:
    sid: str
    raw: dict[str, Any] = field(repr=False)

    @property
    def dispatch_state(self) -> str:
        return str(self.raw.get("dispatchState", ""))

    @property
    def is_done(self) -> bool:
        return bool(self.raw.get("isDone")) or self.dispatch_state in ("DONE", "FAILED")

    @property
    def is_failed(self) -> bool:
        return bool(self.raw.get("isFailed")) or self.dispatch_state == "FAILED"

    @property
    def is_finalized(self) -> bool:
        return bool(self.raw.get("isFinalized"))

    @property
    def is_truncated(self) -> bool:
        return bool(self.raw.get("eventIsTruncated"))

    @property
    def event_count(self) -> int:
        return int(self.raw.get("eventCount") or 0)

    @property
    def result_count(self) -> int:
        return int(self.raw.get("resultCount") or 0)

    @property
    def scan_count(self) -> int:
        return int(self.raw.get("scanCount") or 0)

    @property
    def disk_usage(self) -> int:
        return int(self.raw.get("diskUsage") or 0)

    @property
    def run_duration(self) -> float:
        return float(self.raw.get("runDuration") or 0.0)

    @property
    def messages(self) -> list[dict[str, str]]:
        out = []
        for m in self.raw.get("messages") or []:
            out.append({"type": str(m.get("type", "")).upper(), "text": str(m.get("text", ""))})
        return out

    def problems(self, strict: bool = True) -> list[str]:
        """Reasons this job's results cannot be trusted. Truncation is reported separately."""
        probs: list[str] = []
        if self.is_failed:
            probs.append(f"job failed (dispatchState={self.dispatch_state})")
        if self.is_finalized:
            probs.append("job was finalized before completion (auto-finalize or manual stop)")
        for m in self.messages:
            t = m["type"]
            if t in ("FATAL", "ERROR") or (strict and t == "WARN"):
                probs.append(f"{t}: {m['text']}")
        return probs


class SplunkClient:
    def __init__(
        self,
        base_url: str,
        *,
        token: str | None = None,
        username: str | None = None,
        password: str | None = None,
        verify: bool | str = True,
        connect_timeout: float = 15.0,
        read_timeout: float = 900.0,
        max_retries: int = 6,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self._username = username
        self._password = password
        self._auth_header: str | None = f"Bearer {token}" if token else None
        self._lock = threading.Lock()
        self.max_retries = max_retries
        self._http = httpx.Client(
            base_url=self.base_url,
            verify=verify,
            timeout=httpx.Timeout(read_timeout, connect=connect_timeout),
            http2=False,
        )
        if self._auth_header is None:
            if not (username and password):
                raise AuthError("either a bearer token or username+password is required")
            self.login()

    # ----------------------------------------------------------------- auth
    def login(self) -> None:
        r = self._http.post(
            "/services/auth/login",
            data={"username": self._username, "password": self._password, "output_mode": "json"},
        )
        if r.status_code != 200:
            raise AuthError(f"login failed: HTTP {r.status_code} {r.text[:300]}")
        key = r.json().get("sessionKey")
        if not key:
            raise AuthError("login response carried no sessionKey")
        with self._lock:
            self._auth_header = f"Splunk {key}"
        log.info("logged in to %s as %s", self.base_url, self._username)

    def _headers(self) -> dict[str, str]:
        with self._lock:
            return {"Authorization": self._auth_header or ""}

    # -------------------------------------------------------------- request
    def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
    ) -> httpx.Response:
        attempt = 0
        relogged = False
        while True:
            try:
                r = self._http.request(method, path, params=params, data=data, headers=self._headers())
            except httpx.TransportError as e:
                attempt += 1
                if attempt > self.max_retries:
                    raise RetryExhausted(f"{method} {path}: {e!r} after {attempt} attempts") from e
                self._sleep(attempt, f"{method} {path}: transport error {e!r}")
                continue

            if r.status_code == 401 and self._password and not relogged:
                log.warning("401 on %s %s; re-authenticating", method, path)
                self.login()
                relogged = True
                continue
            if r.status_code in (429, 502, 503, 504):
                attempt += 1
                if attempt > self.max_retries * 3:
                    raise RetryExhausted(f"{method} {path}: HTTP {r.status_code} {r.text[:300]}")
                self._sleep(attempt, f"{method} {path}: HTTP {r.status_code}", cap=60.0)
                continue
            if r.status_code == 404 and "/jobs/" in path:
                raise JobGone(f"{method} {path}: 404")
            if r.status_code >= 400:
                raise SplunkError(f"{method} {path}: HTTP {r.status_code}: {r.text[:500]}")
            return r

    @staticmethod
    def _sleep(attempt: int, why: str, cap: float = 30.0) -> None:
        delay = min(cap, (1.5 ** attempt) + random.random())
        log.warning("%s; retrying in %.1fs (attempt %d)", why, delay, attempt)
        time.sleep(delay)

    def get_json(self, path: str, **params: Any) -> dict[str, Any]:
        params.setdefault("output_mode", "json")
        r = self.request("GET", path, params=params)
        text, _ = decode_tolerant(r.content)
        return json.loads(text)

    # ----------------------------------------------------------- discovery
    def server_info(self) -> dict[str, Any]:
        return self.get_json("/services/server/info")["entry"][0]["content"]

    def limits(self) -> dict[str, Any]:
        """restapi.maxresultrows and search.max_count, with defaults when unreadable."""
        out: dict[str, Any] = {"maxresultrows": 50000, "max_count": 500000, "readable": False}
        try:
            rest = self.get_json("/services/configs/conf-limits/restapi")["entry"][0]["content"]
            srch = self.get_json("/services/configs/conf-limits/search")["entry"][0]["content"]
            out["maxresultrows"] = int(rest.get("maxresultrows", out["maxresultrows"]))
            out["max_count"] = int(srch.get("max_count", out["max_count"]))
            out["readable"] = True
        except SplunkError as e:
            log.info("limits.conf not readable (%s); using defaults", e)
        return out

    def quotas(self) -> dict[str, Any]:
        """Best-effort srchJobsQuota / srchDiskQuota for the current user's roles."""
        out: dict[str, Any] = {"username": None, "roles": [], "srchJobsQuota": None, "srchDiskQuota": None}
        try:
            ctx = self.get_json("/services/authentication/current-context")["entry"][0]["content"]
            out["username"] = ctx.get("username")
            out["roles"] = list(ctx.get("roles") or [])
            jobs, disk = [], []
            for role in out["roles"]:
                c = self.get_json(f"/services/authorization/roles/{role}")["entry"][0]["content"]
                if c.get("srchJobsQuota") is not None:
                    jobs.append(int(c["srchJobsQuota"]))
                if c.get("srchDiskQuota") is not None:
                    disk.append(int(c["srchDiskQuota"]))
            out["srchJobsQuota"] = max(jobs) if jobs else None
            out["srchDiskQuota"] = max(disk) if disk else None
        except SplunkError as e:
            log.info("role quotas not readable (%s)", e)
        return out

    def timeparser(self, expr: str) -> float:
        """Resolve a Splunk time expression to epoch seconds."""
        d = self.get_json("/services/search/timeparser", time=expr)
        value = d.get(expr)
        if value is None:
            raise SplunkError(f"timeparser could not resolve {expr!r}: {d}")
        return datetime.fromisoformat(value).timestamp()

    # ---------------------------------------------------------------- jobs
    def create_job(
        self,
        spl: str,
        earliest: int,
        latest: int,
        *,
        index_latest: int | None = None,
        max_count: int | None = None,
        ttl: int = 3600,
        search_level: str = "fast",
    ) -> str:
        data: dict[str, Any] = {
            "search": spl,
            "exec_mode": "normal",
            "output_mode": "json",
            "earliest_time": str(int(earliest)),
            "latest_time": str(int(latest)),
            "adhoc_search_level": search_level,
            "ttl": str(int(ttl)),
        }
        if index_latest is not None:
            data["index_latest"] = str(int(index_latest))
        if max_count is not None:
            data["max_count"] = str(int(max_count))
        r = self.request("POST", JOBS, data=data)
        sid = r.json().get("sid")
        if not sid:
            raise SplunkError(f"job creation returned no sid: {r.text[:300]}")
        return sid

    def job_status(self, sid: str) -> JobStatus:
        d = self.get_json(f"{JOBS}/{sid}")
        return JobStatus(sid=sid, raw=d["entry"][0]["content"])

    def wait_job(self, sid: str, *, interval: float = 1.0, max_interval: float = 5.0,
                 stop: threading.Event | None = None) -> JobStatus:
        delay = interval
        while True:
            st = self.job_status(sid)
            if st.is_done:
                return st
            if st.raw.get("isZombie"):
                raise SplunkError(f"job {sid} is a zombie (search process died)")
            if stop is not None and stop.wait(delay):
                raise Interrupted()
            if stop is None:
                time.sleep(delay)
            delay = min(max_interval, delay * 1.5)

    def results_page(self, sid: str, offset: int, count: int, *, time_format: str | None = None) -> tuple[list[dict[str, Any]], int]:
        """One page of a finished job's results: (rows, utf8_replacements)."""
        params: dict[str, Any] = {"output_mode": "json", "offset": offset, "count": count}
        if time_format:
            params["output_time_format"] = time_format
        r = self.request("GET", f"{JOBS}/{sid}/results", params=params)
        text, reps = decode_tolerant(r.content)
        return json.loads(text).get("results", []), reps

    def delete_job(self, sid: str) -> None:
        try:
            self.request("DELETE", f"{JOBS}/{sid}")
        except JobGone:
            pass
        except SplunkError as e:
            log.warning("could not delete job %s: %s", sid, e)

    def run_scalar_search(
        self,
        spl: str,
        earliest: int,
        latest: int,
        *,
        index_latest: int | None,
        search_level: str,
        page_size: int,
        strict: bool = True,
        stop: threading.Event | None = None,
    ) -> tuple[list[dict[str, Any]], JobStatus]:
        """Run a transforming search to completion and return all result rows, or raise."""
        sid = self.create_job(spl, earliest, latest, index_latest=index_latest, search_level=search_level,
                              max_count=10_000_000)
        try:
            st = self.wait_job(sid, stop=stop)
            probs = st.problems(strict=strict)
            if probs:
                raise SplunkError(f"search {spl[:80]!r} had problems: {probs}")
            if st.is_truncated:
                raise SplunkError(f"search {spl[:80]!r} was truncated (eventIsTruncated)")
            rows: list[dict[str, Any]] = []
            offset = 0
            while offset < st.result_count:
                page, _ = self.results_page(sid, offset, page_size)
                if not page:
                    break
                rows.extend(page)
                offset += len(page)
            if len(rows) != st.result_count:
                raise SplunkError(f"search {spl[:80]!r}: paged {len(rows)} rows but resultCount is {st.result_count}")
            return rows, st
        finally:
            self.delete_job(sid)

    # -------------------------------------------------------------- export
    def export(
        self,
        spl: str,
        earliest: int,
        latest: int,
        *,
        index_latest: int | None = None,
        search_level: str = "fast",
        time_format: str | None = None,
    ) -> Iterator[tuple[dict[str, Any], int]]:
        """Stream the export endpoint. Yields (object, utf8_replacements) per line.

        The caller must check that a ``lastrow`` object was seen; Splunk returns
        HTTP 200 with an empty body when the search fails fatally.
        """
        data: dict[str, Any] = {
            "search": spl,
            "output_mode": "json",
            "earliest_time": str(int(earliest)),
            "latest_time": str(int(latest)),
            "adhoc_search_level": search_level,
            "preview": "false",
        }
        if index_latest is not None:
            data["index_latest"] = str(int(index_latest))
        if time_format:
            data["output_time_format"] = time_format
        for attempt in range(2):
            with self._http.stream("POST", f"{JOBS}/export", data=data, headers=self._headers()) as r:
                if r.status_code == 401 and self._password and attempt == 0:
                    log.warning("401 on export; re-authenticating")
                    self.login()
                    continue
                if r.status_code != 200:
                    body = r.read()
                    raise SplunkError(f"export HTTP {r.status_code}: {decode_tolerant(body)[0][:500]}")
                yield from self._iter_ndjson(r)
                return

    @staticmethod
    def _iter_ndjson(r: httpx.Response) -> Iterator[tuple[dict[str, Any], int]]:
        buf = b""
        # iter_bytes applies the Content-Encoding (Splunk gzips the export stream when the client advertises
        # Accept-Encoding, which httpx does by default); iter_raw would hand back the compressed bytes.
        for chunk in r.iter_bytes():
            buf += chunk
            while True:
                nl = buf.find(b"\n")
                if nl < 0:
                    break
                line, buf = buf[:nl], buf[nl + 1:]
                if line.strip():
                    text, reps = decode_tolerant(line)
                    yield json.loads(text), reps
        if buf.strip():
            text, reps = decode_tolerant(buf)
            yield json.loads(text), reps

    def close(self) -> None:
        self._http.close()
