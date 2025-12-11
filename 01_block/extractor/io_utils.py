"""Object-oriented file utilities for the extractor package.

Provides safe file operations with consistent error handling and UTF-8 encoding.
"""

from pathlib import Path
from typing import Iterator, List, Optional, Sequence


class DirectoryManager:
    """Manage directory creation for file operations."""

    def ensure_directory(self, path: Path) -> None:
        """Ensure a directory exists, creating it if necessary."""
        if not path.exists():
            path.mkdir(parents=True, exist_ok=True)

    def ensure_parent(self, path: Path) -> None:
        """Ensure the parent directory of a file exists."""
        self.ensure_directory(path.parent)


class TextFileIO:
    """Read and write UTF-8 encoded text files."""

    def __init__(self, directory_manager: Optional[DirectoryManager] = None, *, encoding: str = 'utf-8'):
        self._directory_manager = directory_manager or DirectoryManager()
        self._encoding = encoding

    @property
    def encoding(self) -> str:
        """Expose configured text encoding."""
        return self._encoding

    def ensure_parent(self, path: Path) -> None:
        """Ensure parent directory exists."""
        self._directory_manager.ensure_parent(path)

    def read(self, path: Path, *, errors: str = 'ignore') -> str:
        """Read a text file using configured encoding."""
        return path.read_text(encoding=self._encoding, errors=errors)

    def write(self, path: Path, content: str) -> None:
        """Write a text file, creating parent directories as needed."""
        self.ensure_parent(path)
        path.write_text(content, encoding=self._encoding)

    def atomic_write(self, path: Path, content: str) -> None:
        """Write a text file atomically (write to temp then rename)."""
        self.ensure_parent(path)
        temp_path = path.with_suffix(path.suffix + '.tmp')

        try:
            temp_path.write_text(content, encoding=self._encoding)
            temp_path.replace(path)
        except Exception:
            if temp_path.exists():
                temp_path.unlink()
            raise


class TSVWriter:
    """TSV file writer with header management."""

    def __init__(
        self,
        path: Path,
        header: Sequence[str],
        *,
        text_io: Optional[TextFileIO] = None,
    ):
        self.path = path
        self.header = list(header)
        self._text_io = text_io or DEFAULT_TEXT_IO
        self._handle = None
        self._header_written = False

    def __enter__(self) -> "TSVWriter":
        self._text_io.ensure_parent(self.path)
        self._handle = self.path.open('w', encoding=self._text_io.encoding, newline='')
        self._write_header()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._handle:
            self._handle.close()
            self._handle = None

    def _write_header(self) -> None:
        if not self._header_written:
            self._handle.write('\t'.join(self.header) + '\n')
            self._header_written = True

    def write_row(self, row: Sequence) -> None:
        """Write a data row ensuring header length matches."""
        if len(row) != len(self.header):
            raise ValueError(f"Row length {len(row)} does not match header length {len(self.header)}")

        escaped_values = []
        for val in row:
            val_str = str(val)
            val_str = val_str.replace('\t', ' ').replace('\n', ' ').replace('\r', ' ')
            escaped_values.append(val_str)

        self._handle.write('\t'.join(escaped_values) + '\n')

    def write_rows(self, rows: Sequence[Sequence]) -> None:
        for row in rows:
            self.write_row(row)


class HtmlFileDiscovery:
    """Discover HTML files under a root directory."""

    def __init__(self, root: Path):
        self._root = root

    def discover(self, limit: Optional[int] = None) -> List[Path]:
        if not self._root.exists():
            return []

        files = sorted(self._root.rglob('*.html'))
        if limit is not None and limit > 0:
            files = files[:limit]
        return files

    def iterate(self, limit: Optional[int] = None) -> Iterator[Path]:
        def _generator() -> Iterator[Path]:
            if not self._root.exists():
                return

            count = 0
            for path in sorted(self._root.rglob('*.html')):
                yield path
                count += 1
                if limit is not None and 0 < limit <= count:
                    break

        return _generator()


DEFAULT_DIRECTORY_MANAGER = DirectoryManager()
DEFAULT_TEXT_IO = TextFileIO(DEFAULT_DIRECTORY_MANAGER)


def ensure_directory(path: Path) -> None:
    DEFAULT_DIRECTORY_MANAGER.ensure_directory(path)


def ensure_parent(path: Path) -> None:
    DEFAULT_DIRECTORY_MANAGER.ensure_parent(path)


def read_text_file(path: Path, errors: str = 'ignore') -> str:
    return DEFAULT_TEXT_IO.read(path, errors=errors)


def write_text_file(path: Path, content: str) -> None:
    DEFAULT_TEXT_IO.write(path, content)


def atomic_write_text_file(path: Path, content: str) -> None:
    DEFAULT_TEXT_IO.atomic_write(path, content)


def discover_html_files(root: Path, limit: Optional[int] = None) -> List[Path]:
    return HtmlFileDiscovery(root).discover(limit=limit)


def iter_html_files(root: Path, limit: Optional[int] = None) -> Iterator[Path]:
    return HtmlFileDiscovery(root).iterate(limit=limit)
