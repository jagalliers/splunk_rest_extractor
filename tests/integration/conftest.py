"""Integration tests run only when SPLUNK_URL and a credential are set (see .env)."""
import os

import pytest

from splunk_rest_extractor.client import SplunkClient


def _configured() -> bool:
    return bool(os.environ.get("SPLUNK_TOKEN") or (os.environ.get("SPLUNK_ADMIN_USER") and os.environ.get("SPLUNK_ADMIN_PASS")))


@pytest.fixture(scope="session")
def client():
    if not _configured():
        pytest.skip("no Splunk credentials in environment")
    return SplunkClient(
        os.environ.get("SPLUNK_URL", "https://127.0.0.1:8089"),
        token=os.environ.get("SPLUNK_TOKEN"),
        username=os.environ.get("SPLUNK_ADMIN_USER"),
        password=os.environ.get("SPLUNK_ADMIN_PASS"),
        verify=False,
    )

