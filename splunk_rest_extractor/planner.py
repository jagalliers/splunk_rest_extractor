"""Chunk planning: histogram pass, refinement of hot bins, day-aligned packing."""
from __future__ import annotations

import logging
from dataclasses import dataclass

from .client import SplunkClient
from .spl import histogram_spl
from .timerange import day_bounds

log = logging.getLogger(__name__)

SPAN_LADDER = [86400, 3600, 600, 60, 10, 1]
MAX_BINS = 40_000  # limits.conf [stats] maxresultrows is 50,000 by default; stay under it


@dataclass
class Bin:
    start: int
    end: int
    count: int


@dataclass
class ChunkSpec:
    day: str
    start: int
    end: int
    expected: int | None
    hot: bool = False


def next_span(current: int, min_span: int) -> int | None:
    for s in SPAN_LADDER:
        if s < current and s >= min_span:
            return s
    return None


def initial_span(start: int, end: int, requested: int) -> int:
    """Coarsen the requested span until the histogram has at most MAX_BINS rows."""
    span = requested
    while (end - start) // span > MAX_BINS:
        bigger = [s for s in SPAN_LADDER if s > span]
        if not bigger:
            span *= 4
        else:
            span = min(bigger)
    return span


class Planner:
    def __init__(
        self,
        client: SplunkClient,
        spl: str,
        *,
        pin: int | None,
        target: int,
        min_span: int,
        tz,
        search_level: str,
        page_size: int,
        max_hist_searches: int = 500,
    ) -> None:
        self.client = client
        self.spl = spl
        self.pin = pin
        self.target = target
        self.min_span = max(1, min_span)
        self.tz = tz
        self.search_level = search_level
        self.page_size = page_size
        self.max_hist_searches = max_hist_searches
        self.searches_run = 0

    # ------------------------------------------------------------ histogram
    def histogram(self, start: int, end: int, span: int) -> list[Bin]:
        self.searches_run += 1
        rows, st = self.client.run_scalar_search(
            histogram_spl(self.spl, span), start, end,
            index_latest=self.pin, search_level=self.search_level, page_size=self.page_size,
        )
        bins: list[Bin] = []
        for r in rows:
            bs = int(float(r["bin_start"]))
            bins.append(Bin(max(bs, start), min(bs + span, end), int(r["count"])))
        bins.sort(key=lambda b: b.start)
        log.info("histogram [%d,%d) span=%ds: %d bins, %d events (%.1fs)", start, end, span, len(bins),
                 sum(b.count for b in bins), st.run_duration)
        return bins

    def refined_bins(self, start: int, end: int, initial_span: int) -> list[Bin]:
        bins = self.histogram(start, end, initial_span)
        out: list[Bin] = []
        queue = [(b, initial_span) for b in bins]
        while queue:
            b, span = queue.pop(0)
            if b.count > self.target and (b.end - b.start) > self.min_span:
                sub = next_span(span, self.min_span)
                if sub is not None and self.searches_run < self.max_hist_searches:
                    queue = [(s, sub) for s in self.histogram(b.start, b.end, sub)] + queue
                    continue
                log.warning("bin [%d,%d) has %d events but cannot be refined further", b.start, b.end, b.count)
            out.append(b)
        out.sort(key=lambda x: x.start)
        return out

    # -------------------------------------------------------------- packing
    def pack(self, start: int, end: int, bins: list[Bin]) -> list[ChunkSpec]:
        bins = sorted(bins, key=lambda b: b.start)
        for a, b in zip(bins, bins[1:]):
            if a.end > b.start:
                raise ValueError(f"histogram bins overlap: [{a.start},{a.end}) and [{b.start},{b.end})")
        chunks: list[ChunkSpec] = []
        for day, ds, de in day_bounds(start, end, self.tz):
            day_bins = [b for b in bins if b.start < de and b.end > ds]
            straddle = any(b.start < ds or b.end > de for b in day_bins)
            if straddle:
                log.warning("day %s: a histogram bin straddles the day boundary; expected counts for this day are unknown", day)
            exp = (lambda n: None) if straddle else (lambda n: n)
            cur, cum = ds, 0
            for b in day_bins:
                bs, be = max(b.start, ds), min(b.end, de)
                if b.count > self.target:
                    if cur < bs:  # everything since the last boundary, including bin-less (empty) time
                        chunks.append(ChunkSpec(day, cur, bs, exp(cum)))
                    chunks.append(ChunkSpec(day, bs, be, exp(b.count), hot=True))
                    cur, cum = be, 0
                elif cum > 0 and cum + b.count > self.target:
                    chunks.append(ChunkSpec(day, cur, bs, exp(cum)))
                    cur, cum = bs, b.count
                else:
                    cum += b.count
            if cur < de:
                chunks.append(ChunkSpec(day, cur, de, exp(cum)))
        return chunks

    def fixed(self, start: int, end: int, span: int) -> list[ChunkSpec]:
        chunks: list[ChunkSpec] = []
        for day, ds, de in day_bounds(start, end, self.tz):
            cur = ds
            while cur < de:
                nxt = min(de, cur + span)
                chunks.append(ChunkSpec(day, cur, nxt, None))
                cur = nxt
        return chunks

    def plan(self, start: int, end: int, *, use_histogram: bool, span: int) -> list[ChunkSpec]:
        if not use_histogram:
            return self.fixed(start, end, span)
        eff = initial_span(start, end, span)
        if eff != span:
            log.warning("histogram span raised from %ds to %ds so the range yields at most %d bins", span, eff, MAX_BINS)
        bins = self.refined_bins(start, end, eff)
        return self.pack(start, end, bins)
