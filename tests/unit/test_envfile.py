from pathlib import Path

from splunk_rest_extractor import envfile


def test_parse_handles_comments_quotes_export_and_blanks():
    text = (
        "# comment\n"
        "SPLUNK_URL=https://sh:8089\n"
        'SPLUNK_TOKEN="abc def"\n'
        "export SPLUNK_USERNAME='admin'\n"
        "SPLUNK_PASSWORD=p#ss # trailing comment\n"
        "SPLUNK_CA_BUNDLE=\n"
        "not a line\n"
        "1BAD=x\n"
    )
    assert envfile.parse(text) == {
        "SPLUNK_URL": "https://sh:8089",
        "SPLUNK_TOKEN": "abc def",
        "SPLUNK_USERNAME": "admin",
        "SPLUNK_PASSWORD": "p#ss",
        "SPLUNK_CA_BUNDLE": "",
    }


def test_load_tolerates_bom_and_crlf_and_never_overrides(tmp_path: Path, monkeypatch):
    p = tmp_path / ".env"
    p.write_bytes(b"\xef\xbb\xbfSPLUNK_URL=https://file:8089\r\nSPLUNK_TOKEN=\r\nSPLUNK_USERNAME=fromfile\r\n")
    monkeypatch.setenv("SPLUNK_USERNAME", "fromshell")
    monkeypatch.delenv("SPLUNK_URL", raising=False)
    monkeypatch.delenv("SPLUNK_TOKEN", raising=False)
    applied = envfile.load(p)
    assert applied == {"SPLUNK_URL": "https://file:8089"}
    import os
    assert os.environ["SPLUNK_USERNAME"] == "fromshell"
    assert "SPLUNK_TOKEN" not in os.environ


def test_load_missing_file_is_noop(tmp_path: Path):
    assert envfile.load(tmp_path / "nope.env") == {}
