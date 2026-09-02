import gzip

from splunk_rest_extractor.state import State
from splunk_rest_extractor.validate import coverage_gaps
from splunk_rest_extractor.writer import ChunkWriter, read_chunk_file


def _state(tmp_path):
    st = State(tmp_path / "m.sqlite")
    st.create_run_with_chunks("r", "search x", "sha", 0, 300, None, {}, {}, [("d", 0, 100, None, False, "job", None),
                                                                            ("d", 100, 300, None, False, "job", None)])
    return st


def test_coverage_gaps_detects_holes_overlaps_and_empty(tmp_path):
    st = _state(tmp_path)
    assert coverage_gaps(st.chunks(), 0, 300) == []
    assert coverage_gaps([], 0, 300) == ["no chunks in manifest"]
    st.add_chunks([("d", 300, 400, None, False, "job", None)])
    assert any("ends at 400" in g for g in coverage_gaps(st.chunks(), 0, 300))
    st2 = State(tmp_path / "m2.sqlite")
    st2.create_run_with_chunks("r", "s", "sha", 0, 300, None, {}, {}, [("d", 0, 100, None, False, "job", None),
                                                                        ("d", 150, 300, None, False, "job", None)])
    assert any("gap [100,150)" in g for g in coverage_gaps(st2.chunks(), 0, 300))
    st2.add_chunks([("d", 120, 160, None, False, "job", None)])
    assert any("overlap" in g for g in coverage_gaps(st2.chunks(), 0, 300))


def test_split_chunk_is_idempotent_and_tiles(tmp_path):
    st = _state(tmp_path)
    parent = st.claim_next()
    st.split_chunk(parent.id, [("d", 0, 50), ("d", 50, 100)], "overflow")
    st.split_chunk(parent.id, [("d", 0, 50), ("d", 50, 100)], "overflow")  # crash-and-retry must not duplicate
    assert len(st.children(parent.id)) == 2
    leaf = [c for c in st.chunks() if c.status != "split"]
    assert coverage_gaps(leaf, 0, 300) == []
    assert st.get_chunk(parent.id).status == "split"


def test_reset_for_resume_requeues_and_reports_sids(tmp_path):
    st = _state(tmp_path)
    c = st.claim_next()
    st.update_chunk(c.id, sid="abc")
    st.update_chunk(2, status="failed", attempts=3)
    r = st.reset_for_resume()
    assert r == {"interrupted": 1, "failed": 1, "sids": ["abc"]}
    assert st.counts() == {"pending": 2}
    assert st.get_chunk(c.id).attempts == 0 and st.get_chunk(2).attempts == 0


def test_writer_commit_produces_complete_gzip_and_counts_raw_lines(tmp_path):
    p = tmp_path / "d" / "0-1.raw.gz"
    w = ChunkWriter(p, ["_raw"], "raw")
    w.write_rows([{"_raw": "one"}, {"_raw": "two\nlines"}])
    info = w.commit()
    assert info["written"] == 2 and info["multiline"] == 1 and not p.with_name(p.name + ".part").exists()
    n, sha, _ = read_chunk_file(p)
    assert n == 3 and sha == info["sha256"]
    with gzip.open(p, "rb") as fh:
        assert fh.read() == b"one\ntwo\nlines\n"


def test_writer_abort_removes_part(tmp_path):
    p = tmp_path / "x.jsonl.gz"
    w = ChunkWriter(p, None, "ndjson")
    w.write_rows([{"a": 1}])
    w.abort()
    assert not p.exists() and not p.with_name(p.name + ".part").exists()


def test_ndjson_field_selection(tmp_path):
    p = tmp_path / "x.jsonl.gz"
    w = ChunkWriter(p, ["_time", "_raw"], "ndjson")
    w.write_rows([{"_raw": "r", "_time": "t", "host": "h"}])
    w.commit()
    with gzip.open(p, "rt") as fh:
        assert fh.read() == '{"_time":"t","_raw":"r"}\n'
