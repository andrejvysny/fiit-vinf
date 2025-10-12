"""
CSV writer for per-page metadata
"""

import csv
import json
import os
from pathlib import Path
from typing import Dict, Any, Optional


class PagesCsv:
    """
    Write per-page metadata to CSV file.
    One CSV per run, append-only.
    """

    # Column names in order
    COLUMNS = [
        "timestamp", "url", "page_type", "depth", "referrer",
        "http_status", "content_type", "encoding",
        "content_sha256", "content_bytes", "stored_path",
        "etag", "last_modified", "fetch_latency_ms", "retries",
        "proxy_id", "metadata"
    ]

    def __init__(self, path: str):
        """
        Initialize CSV writer.

        Args:
            path: Path to CSV file
        """
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

        # Open file in append mode
        self._file = open(self.path, 'a', newline='', encoding='utf-8')
        self._writer = csv.writer(self._file, delimiter=',', quoting=csv.QUOTE_MINIMAL)

        # Write header if new file
        if self._file.tell() == 0:
            self._writer.writerow(self.COLUMNS)
            self._file.flush()

        # Statistics
        self.rows_written = 0

    def append(self, row: Dict[str, Any]):
        """
        Append a row to the CSV.

        Args:
            row: Dict with page metadata
        """
        # Convert metadata dict to JSON string if present
        metadata = row.get("metadata")
        if metadata and isinstance(metadata, dict):
            metadata_str = json.dumps(metadata, separators=(',', ':'))
        else:
            metadata_str = metadata if isinstance(metadata, str) else ""

        # Build row in column order
        csv_row = [
            row.get("timestamp", ""),
            row.get("url", ""),
            row.get("page_type", ""),
            row.get("depth", ""),
            row.get("referrer", ""),
            row.get("http_status", ""),
            row.get("content_type", ""),
            row.get("encoding", ""),
            row.get("content_sha256", ""),
            row.get("content_bytes", ""),
            row.get("stored_path", ""),
            row.get("etag", ""),
            row.get("last_modified", ""),
            row.get("fetch_latency_ms", ""),
            row.get("retries", ""),
            row.get("proxy_id", ""),
            metadata_str
        ]

        # Write row
        self._writer.writerow(csv_row)
        self._file.flush()  # Ensure immediate write
        self.rows_written += 1

    def close(self):
        """Close the CSV file"""
        if self._file and not self._file.closed:
            self._file.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def get_stats(self) -> Dict[str, Any]:
        """Get writer statistics"""
        return {
            "path": str(self.path),
            "rows_written": self.rows_written,
            "file_size": self.path.stat().st_size if self.path.exists() else 0
        }