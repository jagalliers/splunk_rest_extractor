import gzip
import json

import httpx

from splunk_rest_extractor.client import SplunkClient

LINES = [
    {"preview": False, "offset": 0, "result": {"_raw": "one", "_cd": "0:1"}, "lastrow": False},
    {"preview": False, "offset": 1, "result": {"_raw": "two é", "_cd": "0:2"}, "lastrow": True},
]


def _client(handler) -> SplunkClient:
    c = SplunkClient("https://splunk.test:8089", token="t")
    c._http = httpx.Client(base_url="https://splunk.test:8089", transport=httpx.MockTransport(handler))
    return c


def test_export_decodes_gzip_content_encoding():
    """Splunk gzips the export stream when Accept-Encoding allows it; the NDJSON parser must see plain bytes."""
    body = "".join(json.dumps(o) + "\n" for o in LINES).encode("utf-8")

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/services/search/v2/jobs/export"
        return httpx.Response(200, content=gzip.compress(body),
                              headers={"Content-Type": "application/json", "Content-Encoding": "gzip"})

    objs = [o for o, _ in _client(handler).export("search index=x", 0, 10)]
    assert objs == LINES


def test_export_plain_body_and_invalid_utf8_are_counted():
    body = b'{"preview":false,"result":{"_raw":"bad \xff\xfe"},"lastrow":true}\n'

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=body, headers={"Content-Type": "application/json"})

    out = list(_client(handler).export("search index=x", 0, 10))
    assert len(out) == 1
    obj, reps = out[0]
    assert obj["lastrow"] is True and reps == 2
