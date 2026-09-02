from datetime import UTC

from splunk_rest_extractor.planner import Bin, Planner, next_span


def mk(target=100, min_span=1):
    return Planner(client=None, spl="search x", pin=None, target=target, min_span=min_span, tz=UTC,
                   search_level="fast", page_size=50000)


def test_next_span_ladder():
    assert next_span(3600, 1) == 600
    assert next_span(60, 10) == 10
    assert next_span(1, 1) is None
    assert next_span(600, 600) is None


def test_pack_tiles_range_and_respects_target():
    p = mk(target=100)
    day = 1534723200
    bins = [Bin(day + i * 3600, day + (i + 1) * 3600, c) for i, c in enumerate([30, 40, 50, 0, 90, 10, 200, 5])]
    chunks = p.pack(day, day + 86400, bins)
    assert chunks[0].start == day and chunks[-1].end == day + 86400
    assert all(a.end == b.start for a, b in zip(chunks, chunks[1:]))
    assert sum(c.expected for c in chunks) == sum(b.count for b in bins)
    for c in chunks:
        if not c.hot:
            assert c.expected <= 100
    hot = [c for c in chunks if c.hot]
    assert len(hot) == 1 and hot[0].expected == 200 and hot[0].start == day + 6 * 3600


def test_pack_empty_day_is_one_chunk():
    p = mk()
    day = 1534723200
    chunks = p.pack(day, day + 2 * 86400, [])
    assert [(c.start, c.end, c.expected) for c in chunks] == [(day, day + 86400, 0), (day + 86400, day + 2 * 86400, 0)]
    assert [c.day for c in chunks] == ["2018-08-20", "2018-08-21"]


def test_fixed_plan():
    p = mk()
    day = 1534723200
    chunks = p.fixed(day + 1000, day + 86400 + 500, 21600)
    assert chunks[0].start == day + 1000 and chunks[-1].end == day + 86400 + 500
    assert all(a.end == b.start for a, b in zip(chunks, chunks[1:]))
    assert all(c.expected is None for c in chunks)
    assert max(c.end - c.start for c in chunks) <= 21600


def test_initial_span_caps_bin_count():
    from splunk_rest_extractor.planner import MAX_BINS, initial_span
    assert initial_span(0, 86400 * 30, 3600) == 3600
    ten_years = 86400 * 3653
    assert initial_span(0, ten_years, 3600) == 86400
    assert ten_years // initial_span(0, ten_years, 60) <= MAX_BINS


def _assert_tiles(chunks, start, end):
    assert chunks[0].start == start and chunks[-1].end == end
    assert all(a.end == b.start for a, b in zip(chunks, chunks[1:]))
    assert all(c.end > c.start for c in chunks)


def test_pack_keeps_empty_time_before_hot_bins():
    p = mk(target=100)
    day = 1534723200
    bins = [Bin(day, day + 3600, 200), Bin(day + 2 * 3600, day + 3 * 3600, 200)]
    chunks = p.pack(day, day + 86400, bins)
    _assert_tiles(chunks, day, day + 86400)
    assert [(c.start - day, c.end - day, c.expected, c.hot) for c in chunks] == [
        (0, 3600, 200, True), (3600, 7200, 0, False), (7200, 10800, 200, True), (10800, 86400, 0, False)]


def test_pack_hot_bin_after_partial_range_start():
    p = mk(target=100)
    day = 1534723200
    start = day + 1800  # range starts mid-hour, first bin is hot and starts later
    bins = [Bin(day + 3600, day + 7200, 500), Bin(day + 7200, day + 10800, 50)]
    chunks = p.pack(start, day + 86400, bins)
    _assert_tiles(chunks, start, day + 86400)
    assert sum(c.expected for c in chunks) == 550


def test_pack_fuzz_always_tiles_and_sums():
    import random

    rnd = random.Random(7)
    p = mk(target=100)
    day = 1534723200
    for _ in range(500):
        start = day + rnd.randrange(0, 3600)
        end = day + rnd.randrange(86400, 3 * 86400)
        bins = []
        t = start
        while t < end:
            span = rnd.choice([60, 600, 3600])
            t1 = min(end, t + span - (t % span) if rnd.random() < 0.5 else t + span)  # aligned or not, never overlapping
            if rnd.random() < 0.6:
                bins.append(Bin(t, t1, rnd.choice([0, 1, 50, 99, 100, 101, 400])))
            t = t1
        bins = [b for b in bins if b.end > b.start]
        chunks = p.pack(start, end, bins)
        _assert_tiles(chunks, start, end)
        day_edges = [day + 86400, day + 2 * 86400]
        straddles = any(b.start < e < b.end for b in bins for e in day_edges)
        if any(c.expected is None for c in chunks):
            assert straddles, "expected counts may only be unknown when a bin crosses a day boundary"
        else:
            assert sum(c.expected for c in chunks) == sum(b.count for b in bins)
        for c in chunks:
            if not c.hot and c.expected is not None:
                assert c.expected <= 100


def test_pack_rejects_overlapping_bins():
    import pytest

    p = mk()
    day = 1534723200
    with pytest.raises(ValueError):
        p.pack(day, day + 86400, [Bin(day, day + 3600, 1), Bin(day + 1800, day + 5400, 1)])
