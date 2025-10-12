"""Lightweight discoveries writer used by the minimal crawler implementation.

This module provides a simple JSONL writer that creates a timestamped
`discoveries-<ts>.jsonl` file in the configured spool directory and
appends discovery items (one JSON object per line). It's intentionally
small and robust for testing the scraper pipeline.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Dict, Any


class DiscoveriesWriter:
    """Write discovery JSONL files into the spool directory.

    Usage:
        w = DiscoveriesWriter('/abs/path/to/workspace/spool/discoveries')
        w.write({'url': 'https://github.com/topics', ...})
        w.close()
    """

    def __init__(self, spool_dir: str) -> None:
        self.dir = Path(spool_dir)
        self.dir.mkdir(parents=True, exist_ok=True)

        ts = int(time.time())
        self.filename = f"discoveries-{ts}.jsonl"
        self.path = self.dir / self.filename
        # Open file in append mode; we'll keep it open for writes
        self._fh = open(self.path, 'a', encoding='utf-8')

    def write(self, item: Dict[str, Any]) -> None:
        """Append a single discovery JSON object as one line.

        The writer flushes and fsyncs the file so other processes (the
        scraper) can reliably see the new content.
        """
        line = json.dumps(item, ensure_ascii=False)
        self._fh.write(line + "\n")
        self._fh.flush()
        try:
            os.fsync(self._fh.fileno())
        except Exception:
            # fsync is best-effort; on some filesystems it may fail
            pass

    def close(self) -> None:
        try:
            if not self._fh.closed:
                self._fh.close()
        except Exception:
            pass

    def filename_str(self) -> str:
        return self.filename
