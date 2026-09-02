from datetime import UTC
from zoneinfo import ZoneInfo

import pytest

from splunk_rest_extractor.timerange import bisect, day_bounds, resolve_time


def test_resolve_epoch_iso_relative():
    assert resolve_time("1534723200", None, round_up=False) == 1534723200
    assert resolve_time("1534723200.4", None, round_up=True) == 1534723201
    assert resolve_time("2018-08-20T00:00:00", None, round_up=False) == 1534723200
    assert resolve_time("2018-08-20T00:00:00-04:00", None, round_up=False) == 1534737600
    assert resolve_time("-1d@d", lambda s: 123.0, round_up=False) == 123


def test_day_bounds_utc_tiles_range():
    start, end = 1534737600, 1534824000  # 04:00 UTC 2018-08-20 -> 04:00 UTC 2018-08-21
    days = day_bounds(start, end, UTC)
    assert days == [("2018-08-20", 1534737600, 1534809600), ("2018-08-21", 1534809600, 1534824000)]
    assert days[0][1] == start and days[-1][2] == end
    assert all(a[2] == b[1] for a, b in zip(days, days[1:]))


def test_day_bounds_dst_transition():
    tz = ZoneInfo("America/New_York")
    # 2026-03-07 00:00 ET .. 2026-03-09 00:00 ET spans the spring-forward day (23h)
    start = 1772859600
    end = start + 24 * 3600 + 23 * 3600
    days = day_bounds(start, end, tz)
    assert [d[0] for d in days] == ["2026-03-07", "2026-03-08"]
    assert days[1][2] - days[1][1] == 23 * 3600
    assert days[-1][2] == end


def test_bisect():
    assert bisect(0, 10) == ((0, 5), (5, 10))
    assert bisect(0, 3) == ((0, 1), (1, 3))
    with pytest.raises(ValueError):
        bisect(5, 6)
