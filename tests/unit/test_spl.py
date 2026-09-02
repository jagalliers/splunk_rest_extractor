from splunk_rest_extractor import spl


def test_normalize_prefixes_search():
    assert spl.normalize("index=foo") == "search index=foo"
    assert spl.normalize("  search index=foo") == "search index=foo"
    assert spl.normalize("| tstats count") == "| tstats count"


def test_rejects_inline_time_modifiers():
    for bad in ["search index=x earliest=-1d", "search index=x latest=now", "search index=x _index_latest=5", "search earliest = 0"]:
        assert any(i.level == "error" for i in spl.validate(bad)), bad
    # a field that merely ends in 'latest' is fine
    assert not any(i.level == "error" for i in spl.validate("search index=x my_latest=1 foo.latest=2"))


def test_rejects_side_effects_and_warns_on_whole_set():
    assert any(i.level == "error" for i in spl.validate("search index=x | outputlookup foo"))
    w = spl.validate("search index=x | head 10 | dedup host")
    assert sum(i.level == "warning" for i in w) == 2


def test_derived_spl():
    assert spl.count_spl("search index=x") == "search index=x | stats count"
    assert spl.with_fields("search index=x", ["_raw", "_time"]) == "search index=x | fields _raw _time"
    assert spl.with_fields("search index=x", None) == "search index=x"
    assert "span=60s" in spl.histogram_spl("search index=x", 60)


def test_rejects_legacy_time_options():
    for bad in ["search index=x starttime=08/20/2018:00:00:00", "search index=x endtimeu=1534723200", "search x searchtimespanhours=1"]:
        assert any(i.level == "error" for i in spl.validate(bad)), bad
    assert spl.is_whole_set("search index=x | stats count by host")
    assert not spl.is_whole_set("search index=x | fields _raw")
