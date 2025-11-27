"""Utility helpers for manifests and structured logging."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict


def sha1_hexdigest(path: Path, chunk_size: int = 1024 * 1024) -> str:
    """Return the SHA-1 hex digest of ``path``."""
    import hashlib

    sha1 = hashlib.sha1()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            sha1.update(chunk)
    return sha1.hexdigest()


def write_manifest(path: Path, payload: Dict[str, Any]) -> None:
    """Atomically write a JSON manifest."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp_path.replace(path)


class StructuredLogger:
    """Append-only JSONL logger for pipeline telemetry."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = self.path.open("a", encoding="utf-8")

    def log(self, event: str, **payload: Any) -> None:
        record = {"ts": time.time(), "event": event, **payload}
        self._handle.write(json.dumps(record, ensure_ascii=False) + "\n")
        self._handle.flush()

    def close(self) -> None:
        if not self._handle.closed:
            self._handle.close()

    def __enter__(self) -> "StructuredLogger":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
