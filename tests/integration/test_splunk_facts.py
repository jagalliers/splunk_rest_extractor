"""Pins the Splunk behaviours DESIGN.md section 1 relies on. If a Splunk upgrade changes one, this fails loudly.

Uses index=botsv3 (static) and index=_internal (live). Run: uv run pytest tests/integration -v
"""
import time

import pytest


def count(client, spl, a, b, **kw):
    rows, st = client.run_scalar_search(f"{spl} | stats count", a, b, index_latest=None, search_level="fast", page_size=50000, **kw)
    return int(rows[0]["count"]) if rows else 0, st

BOTS_DAY = (1534737600, 1534824000)  # 2018-08-20 00:00 EDT .. +24h, 2,083,055 events through the search pipeline


def test_fact1_results_page_is_capped_at_maxresultrows(client):
    sid = client.create_job("search index=botsv3", 1534770000, 1534773600, max_count=1_000_000)
    try:
        st = client.wait_job(sid)
        assert st.result_count > 50000
        rows, _ = client.results_page(sid, 0, 100000)
        assert len(rows) == client.limits()["maxresultrows"]
    finally:
        client.delete_job(sid)


def test_fact2_truncation_is_silent_and_stops_scanning(client):
    sid = client.create_job("search index=botsv3", *BOTS_DAY)  # limits.conf max_count applies
    try:
        st = client.wait_job(sid)
        assert st.is_truncated
        assert not st.is_failed and not st.is_finalized and not st.problems()
        assert st.event_count < 2_083_055, "eventCount must not be trusted on a truncated job"
    finally:
        client.delete_job(sid)


def test_fact3_job_max_count_lifts_the_cap(client):
    sid = client.create_job("search index=botsv3 | fields _cd", 1534770000, 1534773600, max_count=1_000_000)
    try:
        st = client.wait_job(sid)
        assert not st.is_truncated and st.result_count == 443808
    finally:
        client.delete_job(sid)


def test_fact4_boundaries_are_half_open_and_partition(client):
    e = 1534746578  # a second on botsv3 with exactly one event
    inwin, _ = count(client, "search index=botsv3", e, e + 1)
    left, _ = count(client, "search index=botsv3", e - 600, e)
    right, _ = count(client, "search index=botsv3", e + 1, e + 601)
    whole, _ = count(client, "search index=botsv3", e - 600, e + 601)
    assert inwin == 1 and left + inwin + right == whole
    assert count(client, "search index=botsv3", e - 600, e + 1)[0] == left + inwin


def test_fact5_inline_time_modifier_overrides_params(client):
    a, b = 1534770000, 1534773600
    n_param, _ = count(client, "search index=botsv3", a, b)
    n_inline, _ = count(client, f"search index=botsv3 earliest={a} latest={a + 60}", a, b)
    assert n_inline < n_param


def test_fact6_index_latest_param_pins_live_data(client):
    pin = int(time.time()) - 60
    rows1, _ = client.run_scalar_search("search index=_internal | stats count", 0, int(time.time()) + 86400, index_latest=pin, search_level="fast", page_size=50000)
    time.sleep(2)
    rows2, _ = client.run_scalar_search("search index=_internal | stats count", 0, int(time.time()) + 86400, index_latest=pin, search_level="fast", page_size=50000)
    assert rows1[0]["count"] == rows2[0]["count"]


def test_fact7_pipeline_count_differs_from_scan_and_tstats(client):
    n, st = count(client, "search index=botsv3", *BOTS_DAY)
    rows, _ = client.run_scalar_search("| tstats count where index=botsv3", *BOTS_DAY, index_latest=None, search_level="fast", page_size=50000)
    assert n == 2_083_055 and st.scan_count < n and int(rows[0]["count"]) == st.scan_count


def test_fact9_export_swallows_fatal_errors_and_marks_lastrow(client):
    objs = [o for o, _ in client.export("search index=botsv3 | head 3 | lookup nosuchlookup_xyz host", *BOTS_DAY)]
    assert objs == []
    objs = [o for o, _ in client.export("search index=botsv3 sourcetype=does_not_exist_xyz", *BOTS_DAY)]
    assert len(objs) == 1 and objs[0].get("lastrow") is True and "result" not in objs[0]


def test_fact10_failed_job_reports_typed_messages(client):
    sid = client.create_job("search index=botsv3 | head 3 | lookup nosuchlookup_xyz host", *BOTS_DAY)
    try:
        st = client.wait_job(sid)
        assert st.is_failed and any(m["type"] in ("FATAL", "ERROR") for m in st.messages)
        assert st.problems()
    finally:
        client.delete_job(sid)


def test_fact11_invalid_utf8_passes_through(client):
    sid = client.create_job("search index=botsv3 sourcetype=stream:udp | fields _raw", 1534770000, 1534773600, max_count=1_000_000)
    try:
        client.wait_job(sid)
        _, reps = client.results_page(sid, 0, 50000)
        assert reps > 0
    finally:
        client.delete_job(sid)


def test_fact12_timeparser(client):
    assert abs(client.timeparser("now") - time.time()) < 60
    assert client.timeparser("1534737600") == 1534737600


def test_time_format_is_identical_on_both_endpoints(client):
    fmt = "%Y-%m-%dT%H:%M:%S.%3N%:z"
    a, b = 1534770000, 1534770010
    sid = client.create_job("search index=botsv3 | fields _time _cd", a, b, max_count=100000)
    try:
        client.wait_job(sid)
        rows, _ = client.results_page(sid, 0, 50000, time_format=fmt)
    finally:
        client.delete_job(sid)
    exp = [o["result"] for o, _ in client.export("search index=botsv3 | fields _time _cd", a, b, time_format=fmt) if "result" in o]
    assert sorted((r["_cd"], r["_time"]) for r in rows) == sorted((r["_cd"], r["_time"]) for r in exp)
