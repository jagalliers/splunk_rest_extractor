"""Minimal .env loader so credentials need no shell-specific export dance.

Accepts `KEY=VALUE` lines, blank lines, `#` comments, an optional `export ` prefix, and single- or double-quoted
values. No variable expansion. A UTF-8 BOM (Notepad, some editors) is tolerated. Existing environment variables
always win, and a blank value is treated as unset, so a copied template with empty lines is harmless.
"""
from __future__ import annotations

import os
from pathlib import Path


def parse(text: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):].lstrip()
        key, sep, value = line.partition("=")
        key = key.strip()
        if not sep or not key.isidentifier():
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        else:
            value = value.split(" #", 1)[0].rstrip()
        out[key] = value
    return out


def load(path: Path) -> dict[str, str]:
    """Apply `path` to os.environ; return what was applied. A missing file applies nothing."""
    if not path.is_file():
        return {}
    applied: dict[str, str] = {}
    for key, value in parse(path.read_text(encoding="utf-8-sig")).items():
        if value == "" or os.environ.get(key):
            continue
        os.environ[key] = value
        applied[key] = value
    return applied
