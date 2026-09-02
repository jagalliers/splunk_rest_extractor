"""Time inputs -> epoch integers, day boundaries in a time zone, bisection."""
from __future__ import annotations

import math
import re
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

EPOCH_RE = re.compile(r"^\d+(\.\d+)?$")


def resolve_time(value: str, resolver, *, round_up: bool) -> int:
    """Accept epoch, ISO-8601 (naive = UTC), or a Splunk relative expression."""
    v = value.strip()
    if EPOCH_RE.match(v):
        f = float(v)
    else:
        try:
            dt = datetime.fromisoformat(v)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            f = dt.timestamp()
        except ValueError:
            f = float(resolver(v))
    return int(math.ceil(f)) if round_up else int(math.floor(f))


def get_tz(name: str):
    return timezone.utc if name.upper() == "UTC" else ZoneInfo(name)


def day_of(epoch: int, tz) -> str:
    return datetime.fromtimestamp(epoch, tz).strftime("%Y-%m-%d")


def day_bounds(start: int, end: int, tz) -> list[tuple[str, int, int]]:
    """Half-open day intervals in `tz` that tile [start, end)."""
    out: list[tuple[str, int, int]] = []
    if end <= start:
        return out
    cur_dt = datetime.fromtimestamp(start, tz)
    day_start_dt = cur_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    cur = start
    while cur < end:
        next_day_dt = (day_start_dt + timedelta(days=1, hours=12)).replace(hour=0, minute=0, second=0, microsecond=0)
        next_day = int(next_day_dt.timestamp())
        seg_end = min(end, next_day)
        out.append((day_start_dt.strftime("%Y-%m-%d"), cur, seg_end))
        cur = seg_end
        day_start_dt = next_day_dt
    return out


def bisect(start: int, end: int) -> tuple[tuple[int, int], tuple[int, int]]:
    if end - start < 2:
        raise ValueError(f"cannot bisect [{start},{end})")
    mid = start + (end - start) // 2
    return (start, mid), (mid, end)
