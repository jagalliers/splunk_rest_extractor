"""Chunk file writer: gzip NDJSON (or raw lines), .part + fsync + atomic rename, sha256."""
from __future__ import annotations

import gzip
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Iterable


def chunk_path(data_dir: Path, day: str, start: int, end: int, fmt: str) -> Path:
    ext = "jsonl.gz" if fmt == "ndjson" else "raw.gz"
    return data_dir / day / f"{start}-{end}.{ext}"


class ChunkWriter:
    def __init__(self, final_path: Path, fields: list[str] | None, fmt: str = "ndjson") -> None:
        self.final_path = final_path
        self.part_path = final_path.with_name(final_path.name + ".part")
        self.fields = fields
        self.fmt = fmt
        self.written = 0
        self.extra_lines = 0  # newline characters inside _raw in raw mode (physical lines beyond one per event)
        self._sha = hashlib.sha256()
        self._bytes = 0
        final_path.parent.mkdir(parents=True, exist_ok=True)
        # Keep the raw file handle so the gzip trailer is on disk before the fsync and the rename.
        self._raw = open(self.part_path, "wb")
        self._fh = gzip.GzipFile(fileobj=self._raw, mode="wb", compresslevel=6)

    def _encode(self, row: dict[str, Any]) -> bytes:
        if self.fmt == "raw":
            raw = row.get("_raw", "")
            self.extra_lines += raw.count("\n")
            return (raw + "\n").encode("utf-8", errors="replace")
        if self.fields:
            row = {k: row[k] for k in self.fields if k in row}
        return (json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8", errors="replace")

    def write_rows(self, rows: Iterable[dict[str, Any]]) -> int:
        n = 0
        for row in rows:
            b = self._encode(row)
            self._fh.write(b)
            self._sha.update(b)
            self._bytes += len(b)
            n += 1
        self.written += n
        return n

    def commit(self) -> dict[str, Any]:
        self._fh.close()          # writes the final deflate block and the gzip trailer
        self._raw.flush()
        os.fsync(self._raw.fileno())
        self._raw.close()
        os.replace(self.part_path, self.final_path)
        dfd = os.open(str(self.final_path.parent), os.O_RDONLY)
        try:
            os.fsync(dfd)
        finally:
            os.close(dfd)
        return {
            "written": self.written,
            "bytes": self._bytes,
            "sha256": self._sha.hexdigest(),
            "multiline": self.extra_lines,
            "path": str(self.final_path),
        }

    def abort(self) -> None:
        try:
            self._fh.close()
        except Exception:  # noqa: BLE001
            pass
        try:
            self._raw.close()
        except Exception:  # noqa: BLE001
            pass
        try:
            self.part_path.unlink()
        except FileNotFoundError:
            pass


def read_chunk_file(path: Path) -> tuple[int, str, int]:
    """(line_count, sha256 of uncompressed bytes, uncompressed bytes). Raises on a corrupt file."""
    sha = hashlib.sha256()
    n = 0
    size = 0
    with gzip.open(path, "rb") as fh:
        for line in fh:
            sha.update(line)
            size += len(line)
            n += 1
    return n, sha.hexdigest(), size
