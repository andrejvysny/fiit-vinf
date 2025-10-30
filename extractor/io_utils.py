"""I/O utilities for reading and writing files in the extractor.

Provides safe file operations with consistent error handling and UTF-8 encoding.
"""

import os
from pathlib import Path
from typing import Iterator, List, Optional


def ensure_directory(path: Path) -> None:
    """Ensure a directory exists, creating it if necessary.

    Args:
        path: Directory path
    """
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)


def ensure_parent(path: Path) -> None:
    """Ensure the parent directory of a file exists.

    Args:
        path: File path
    """
    parent = path.parent
    if not parent.exists():
        parent.mkdir(parents=True, exist_ok=True)


def read_text_file(path: Path, errors: str = 'ignore') -> str:
    """Read a text file with UTF-8 encoding.

    Args:
        path: Path to file
        errors: Error handling strategy ('ignore', 'replace', 'strict')

    Returns:
        File contents as string
    """
    return path.read_text(encoding='utf-8', errors=errors)


def write_text_file(path: Path, content: str) -> None:
    """Write a text file with UTF-8 encoding.

    Creates parent directories if needed.

    Args:
        path: Path to file
        content: Content to write
    """
    ensure_parent(path)
    path.write_text(content, encoding='utf-8')


def atomic_write_text_file(path: Path, content: str) -> None:
    """Write a text file atomically (write to temp, then rename).

    Prevents partial writes from leaving corrupted files.

    Args:
        path: Path to file
        content: Content to write
    """
    ensure_parent(path)
    temp_path = path.with_suffix(path.suffix + '.tmp')

    try:
        temp_path.write_text(content, encoding='utf-8')
        temp_path.replace(path)  # Atomic on POSIX systems
    except Exception:
        # Clean up temp file on error
        if temp_path.exists():
            temp_path.unlink()
        raise


class TSVWriter:
    """TSV file writer with header management.

    Ensures header is written exactly once and rows are properly formatted.
    """

    def __init__(self, path: Path, header: List[str]):
        """Initialize TSV writer.

        Args:
            path: Path to TSV file
            header: List of column names
        """
        self.path = path
        self.header = header
        self._handle = None
        self._header_written = False

    def __enter__(self):
        ensure_parent(self.path)
        self._handle = self.path.open('w', encoding='utf-8', newline='')
        self._write_header()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._handle:
            self._handle.close()
            self._handle = None

    def _write_header(self) -> None:
        """Write the header row."""
        if not self._header_written:
            self._handle.write('\t'.join(self.header) + '\n')
            self._header_written = True

    def write_row(self, row: tuple) -> None:
        """Write a data row.

        Args:
            row: Tuple of values (length must match header)
        """
        if len(row) != len(self.header):
            raise ValueError(f"Row length {len(row)} does not match header length {len(self.header)}")

        # Escape tabs and newlines in values by replacing them with spaces
        # This is a simple escaping strategy; TSV has no standard escaping
        escaped = []
        for val in row:
            val_str = str(val)
            val_str = val_str.replace('\t', ' ').replace('\n', ' ').replace('\r', ' ')
            escaped.append(val_str)

        self._handle.write('\t'.join(escaped) + '\n')

    def write_rows(self, rows: List[tuple]) -> None:
        """Write multiple data rows.

        Args:
            rows: List of row tuples
        """
        for row in rows:
            self.write_row(row)


def discover_html_files(root: Path, limit: Optional[int] = None) -> List[Path]:
    """Discover all HTML files under a root directory.

    Args:
        root: Root directory to search
        limit: Optional maximum number of files to return

    Returns:
        Sorted list of HTML file paths
    """
    if not root.exists():
        return []

    files = sorted(root.rglob('*.html'))

    if limit is not None and limit > 0:
        files = files[:limit]

    return files


def iter_html_files(root: Path, limit: Optional[int] = None) -> Iterator[Path]:
    """Iterate over HTML files under a root directory.

    Args:
        root: Root directory to search
        limit: Optional maximum number of files to yield

    Yields:
        HTML file paths
    """
    if not root.exists():
        return

    count = 0
    for path in sorted(root.rglob('*.html')):
        yield path
        count += 1
        if limit is not None and count >= limit:
            break
