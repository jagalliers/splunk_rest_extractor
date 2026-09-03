"""Every failure must end as one ERROR line saying what went wrong plus a hint, with a documented exit code."""
import argparse
import json
import logging
import sqlite3
from zoneinfo import ZoneInfoNotFoundError

import httpx
import pytest

from splunk_rest_extractor import cli
from splunk_rest_extractor.client import AuthError, RetryExhausted, SplunkClient, SplunkError


def _args(**kw) -> argparse.Namespace:
    base = {"url": "https://splunk.test:8089", "token": None, "username": None, "cmd": "plan", "tz": "UTC", "out": None}
    base.update(kw)
    return argparse.Namespace(**base)


def _connect_error(text: str) -> httpx.ConnectError:
    return httpx.ConnectError(text, request=httpx.Request("GET", "https://splunk.test:8089/x"))


# ------------------------------------------------------------------ explain()
@pytest.mark.parametrize("exc, kw, message, hint, code, traceback", [
    (cli.CliError("boom", hint="do this", code=2), {}, "boom", "do this", 2, False),
    (KeyboardInterrupt(), {"cmd": "run"}, "interrupted", "re-run the same command", 130, False),
    (KeyboardInterrupt(), {"cmd": "plan"}, "interrupted", None, 130, False),
    (AuthError("either a bearer token or username+password is required"), {}, "either a bearer token", "SPLUNK_TOKEN", 2, False),
    (SplunkError("GET /x: HTTP 401: nope", status=401), {"token": "t"}, "rejected the credentials (HTTP 401)", "Bearer", 1, False),
    (SplunkError("GET /x: HTTP 401: nope", status=401), {"username": "u"}, "rejected the credentials (HTTP 401)", "SPLUNK_PASSWORD", 1, False),
    (SplunkError("GET /x: HTTP 403: denied", status=403), {}, "HTTP 403", "capability", 1, False),
    (SplunkError("POST /jobs: HTTP 400: bad", status=400), {}, "HTTP 400", "objected", 1, False),
    (SplunkError("job x is a zombie"), {}, "zombie", None, 1, False),
    (_connect_error("[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed"), {}, "certificate", "--ca-bundle", 1, False),
    (_connect_error("[SSL: WRONG_VERSION_NUMBER] wrong version number"), {}, "not speaking TLS", "8089", 1, False),
    (_connect_error("[Errno 11001] getaddrinfo failed"), {}, "resolve the host", "host", 1, False),
    (httpx.ConnectTimeout("timed out"), {}, "nothing answered", "closed or filtered", 1, False),
    (httpx.ReadTimeout("timed out"), {}, "stopped responding", "--read-timeout", 1, False),
    (httpx.UnsupportedProtocol("Request URL is missing a protocol"), {"url": "host:8089"}, "invalid URL", "https://host:8089", 1, False),
    (json.JSONDecodeError("x", "<html>", 0), {}, "other than JSON", "port 8000", 1, False),
    (ZoneInfoNotFoundError("Nowhere/Land"), {"tz": "Nowhere/Land"}, "Nowhere/Land", "IANA", 2, False),
    (sqlite3.OperationalError("unable to open database file"), {}, "run manifest", "writable", 1, False),
    (FileNotFoundError(2, "No such file", "x.spl"), {}, "x.spl", "exists", 1, False),
    (RuntimeError("kaboom"), {}, "unexpected error (this is a bug): RuntimeError: kaboom", "traceback follows", 1, True),
])
def test_explain_table(exc, kw, message, hint, code, traceback):
    d = cli.explain(exc, _args(**kw))
    assert message in d.message
    if hint is None:
        assert d.hint is None
    else:
        assert hint in d.hint
    assert d.code == code
    assert d.traceback is traceback


def test_explain_unwraps_retry_exhausted_cause():
    try:
        raise RetryExhausted("GET /x: ConnectTimeout after 7 attempts") from httpx.ConnectTimeout("timed out")
    except RetryExhausted as e:
        d = cli.explain(e, _args())
    assert d.message.startswith("gave up: nothing answered")
    assert "8089" in d.hint


# ------------------------------------------------------------------ main()
def _errors(caplog) -> list[str]:
    return [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]


def test_anticipated_error_is_one_line_plus_hint_no_traceback(monkeypatch, caplog):
    def boom(a):
        raise cli.CliError("thing went wrong", hint="try this", code=2)
    monkeypatch.setattr(cli, "cmd_plan", boom)
    code = cli.main(["plan", "--spl", "index=x", "--earliest", "-1h", "--latest", "now"])
    assert code == 2
    assert _errors(caplog) == ["thing went wrong", "   -> try this"]
    assert "Traceback" not in caplog.text


def test_unexpected_error_says_bug_and_shows_traceback(monkeypatch, caplog):
    def boom(a):
        raise RuntimeError("kaboom")
    monkeypatch.setattr(cli, "cmd_plan", boom)
    code = cli.main(["plan", "--spl", "index=x", "--earliest", "-1h", "--latest", "now"])
    assert code == 1
    msgs = _errors(caplog)
    assert msgs[0] == "unexpected error (this is a bug): RuntimeError: kaboom"
    assert msgs[1].startswith("   -> traceback follows")
    assert "Traceback (most recent call last)" in caplog.text and "kaboom" in caplog.text


def test_ctrl_c_outside_run_is_clean(monkeypatch, caplog):
    def boom(a):
        raise KeyboardInterrupt
    monkeypatch.setattr(cli, "cmd_plan", boom)
    assert cli.main(["plan", "--spl", "index=x", "--earliest", "-1h", "--latest", "now"]) == 130
    assert _errors(caplog) == ["interrupted"]


def test_argparse_errors_still_exit_2(capsys):
    with pytest.raises(SystemExit) as e:
        cli.main(["run", "--spl", "index=x", "--earliest", "-1h", "--latest", "now"])  # no --out
    assert e.value.code == 2
    assert "--out" in capsys.readouterr().err


def test_logging_handlers_are_removed_after_main(monkeypatch):
    monkeypatch.setattr(cli, "cmd_plan", lambda a: 0)
    before = list(logging.getLogger().handlers)
    cli.main(["plan", "--spl", "index=x", "--earliest", "-1h", "--latest", "now"])
    assert logging.getLogger().handlers == before


# ------------------------------------------------------------ command paths
def test_status_on_missing_directory(tmp_path, caplog):
    out = tmp_path / "nope"
    assert cli.main(["status", "--out", str(out)]) == 1
    assert _errors(caplog)[0] == f"no run directory at {out}"
    assert not out.exists()


def test_status_on_directory_without_manifest_creates_nothing(tmp_path, caplog):
    assert cli.main(["status", "--out", str(tmp_path)]) == 1
    assert _errors(caplog)[0] == f"{tmp_path} is not a run directory (no manifest.sqlite)"
    assert not (tmp_path / "manifest.sqlite").exists()


def test_empty_spl_is_a_usage_error(caplog):
    assert cli.main(["plan", "--spl", "   ", "--earliest", "-1h", "--latest", "now"]) == 2
    assert _errors(caplog)[0] == "the search is empty"


def test_missing_spl_file_is_a_usage_error(tmp_path, caplog):
    missing = tmp_path / "missing.spl"
    assert cli.main(["plan", "--spl-file", str(missing), "--earliest", "-1h", "--latest", "now"]) == 2
    assert _errors(caplog)[0].startswith(f"cannot read --spl-file {missing}")


def test_spl_with_inline_time_is_rejected_with_the_reason_first(caplog):
    assert cli.main(["plan", "--spl", "index=x earliest=-1d", "--earliest", "-1h", "--latest", "now"]) == 2
    msgs = _errors(caplog)
    assert msgs[0].startswith("SPL error:")
    assert msgs[-2] == "the search was rejected (1 SPL error(s) above)"


def test_no_credentials_is_explained(tmp_path, monkeypatch, caplog):
    monkeypatch.chdir(tmp_path)  # no .env here
    for var in ("SPLUNK_TOKEN", "SPLUNK_USERNAME", "SPLUNK_PASSWORD", "SPLUNK_ADMIN_USER", "SPLUNK_ADMIN_PASS"):
        monkeypatch.delenv(var, raising=False)
    assert cli.main(["plan", "--spl", "index=x", "--earliest", "-1h", "--latest", "now"]) == 2
    assert _errors(caplog) == ["either a bearer token or username+password is required",
                               "   -> set SPLUNK_TOKEN, or SPLUNK_USERNAME and SPLUNK_PASSWORD, in .env or the environment "
                               "(or pass --token / --username / --password)"]


def test_bad_time_expression_is_a_usage_error(caplog):
    class FakeClient:
        def timeparser(self, expr):
            raise SplunkError('GET /services/search/timeparser: HTTP 400: {"messages":[{"text":"Invalid time."}]}', status=400)

    with pytest.raises(cli.CliError) as e:
        cli.resolve_range(FakeClient(), argparse.Namespace(earliest="-90x", latest="now"))
    assert str(e.value) == "Splunk could not parse --earliest '-90x'"
    assert e.value.code == 2


def test_auth_failure_during_time_resolution_is_not_blamed_on_the_expression():
    class FakeClient:
        def timeparser(self, expr):
            raise SplunkError("GET /services/search/timeparser: HTTP 401: nope", status=401)

    with pytest.raises(SplunkError) as e:
        cli.resolve_range(FakeClient(), argparse.Namespace(earliest="-1h", latest="now"))
    assert e.value.status == 401


# ------------------------------------------------------------------ client
def _client(handler) -> SplunkClient:
    c = SplunkClient("https://splunk.test:8089", token="t")
    c._http = httpx.Client(base_url="https://splunk.test:8089", transport=httpx.MockTransport(handler))
    c._sleep = lambda *a, **k: None
    return c


def test_http_status_is_carried_on_splunk_error():
    def handler(request):
        return httpx.Response(401, json={"messages": [{"type": "WARN", "text": "call not properly authenticated"}]})
    with pytest.raises(SplunkError) as e:
        _client(handler).get_json("/services/server/info")
    assert e.value.status == 401


def test_transport_error_before_first_response_is_not_retried():
    calls = 0

    def handler(request):
        nonlocal calls
        calls += 1
        raise httpx.ConnectError("[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed", request=request)
    with pytest.raises(httpx.ConnectError):
        _client(handler).get_json("/services/server/info")
    assert calls == 1


def test_transport_error_after_a_response_is_retried_then_wrapped():
    calls = 0

    def handler(request):
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(200, json={"entry": [{"content": {}}]})
        raise httpx.ReadTimeout("timed out", request=request)
    c = _client(handler)
    c.max_retries = 2
    c.get_json("/services/server/info")
    with pytest.raises(RetryExhausted) as e:
        c.get_json("/services/server/info")
    assert calls == 1 + 3  # first success, then max_retries + 1 attempts
    assert isinstance(e.value.__cause__, httpx.ReadTimeout)
