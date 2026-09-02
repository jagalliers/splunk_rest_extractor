"""SPL guardrails: the search must be time-partitionable and must not carry time modifiers."""
from __future__ import annotations

import re
from dataclasses import dataclass

TIME_MODIFIER_RE = re.compile(
    r"(?<![\w.])(_index_earliest|_index_latest|earliest|latest|starttimeu|endtimeu|starttime|endtime|"
    r"searchtimespanhours|searchtimespanminutes|searchtimespandays|searchtimespanmonths)\s*=", re.I)
SIDE_EFFECT_RE = re.compile(r"\|\s*(outputlookup|collect|delete|sendemail|outputcsv|mcollect|meventcollect)\b", re.I)
WHOLE_SET_RE = re.compile(
    r"\|\s*(head|tail|dedup|sort|streamstats|eventstats|transaction|stats|top|rare|chart|timechart|"
    r"table|reverse|uniq|autoregress|delta|accum|trendline|predict|localize|concurrency)\b",
    re.I,
)
TIME_IGNORING_RE = re.compile(r"^\|\s*(inputlookup|inputcsv|makeresults|gentimes|rest|dbxquery|ldapsearch)\b", re.I)


@dataclass
class Issue:
    level: str  # "error" | "warning"
    text: str


def normalize(spl: str) -> str:
    s = spl.strip()
    if not s:
        raise ValueError("empty SPL")
    if s.startswith("|"):
        return s
    if re.match(r"^search\b", s, re.I):
        return s
    return "search " + s


def is_whole_set(spl: str) -> bool:
    return WHOLE_SET_RE.search(spl) is not None


def validate(spl: str) -> list[Issue]:
    issues: list[Issue] = []
    m = TIME_MODIFIER_RE.search(spl)
    if m:
        issues.append(Issue("error", (
            f"SPL contains the time modifier {m.group(1)!r}. Inline time modifiers override the REST "
            "earliest_time/latest_time parameters, so every chunk would silently search the same window. "
            "Remove it and pass the range with --earliest/--latest."
        )))
    m = SIDE_EFFECT_RE.search(spl)
    if m:
        issues.append(Issue("error", f"SPL contains the side-effecting command {m.group(1)!r}; refusing to run it once per chunk."))
    for m in WHOLE_SET_RE.finditer(spl):
        issues.append(Issue("warning", (
            f"'{m.group(1)}' operates on the whole result set. The SPL is run once per time chunk, so the "
            "concatenated output will not equal a single run of this search."
        )))
    if TIME_IGNORING_RE.match(spl):
        issues.append(Issue("warning", f"generating command {spl.split()[1]!r} ignores the search time range; chunking will repeat its output."))
    elif spl.startswith("|"):
        issues.append(Issue("warning", "generating command at the head of the pipeline; verify it honours earliest_time/latest_time and index_latest."))
    return issues


def with_fields(spl: str, fields: list[str] | None) -> str:
    if not fields:
        return spl
    return f"{spl} | fields {' '.join(fields)}"


def histogram_spl(spl: str, span_seconds: int) -> str:
    return f"{spl} | bin _time span={int(span_seconds)}s | stats count by _time | eval bin_start=_time | fields bin_start count"


def count_spl(spl: str) -> str:
    return f"{spl} | stats count"
