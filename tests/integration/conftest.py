"""Integration tests run only when SPLUNK_URL and a credential are set, in the environment or in ./.env."""
import os
from pathlib import Path

import pytest

from splunk_rest_extractor import envfile
from splunk_rest_extractor.client import SplunkClient

envfile.load(Path(".env"))


def _env(*names: str) -> str | None:
    for n in names:
        if os.environ.get(n):
            return os.environ[n]
    return None


def _configured() -> bool:
    return bool(_env("SPLUNK_TOKEN") or (_env("SPLUNK_USERNAME", "SPLUNK_ADMIN_USER") and _env("SPLUNK_PASSWORD", "SPLUNK_ADMIN_PASS")))


@pytest.fixture(scope="session")
def client():
    if not _configured():
        pytest.skip("no Splunk credentials in environment")
    return SplunkClient(
        os.environ.get("SPLUNK_URL", "https://127.0.0.1:8089"),
        token=os.environ.get("SPLUNK_TOKEN"),
        username=_env("SPLUNK_USERNAME", "SPLUNK_ADMIN_USER"),
        password=_env("SPLUNK_PASSWORD", "SPLUNK_ADMIN_PASS"),
        verify=False,
    )

