"""File utilities for the extractor package."""

from pathlib import Path
from typing import List, Optional


class DirectoryManager:
    """Manage directory creation for file operations."""

    def ensure_directory(self, path: Path) -> None:
        if not path.exists():
            path.mkdir(parents=True, exist_ok=True)

    def ensure_parent(self, path: Path) -> None:
        self.ensure_directory(path.parent)


class TextFileIO:
    """Read and write UTF-8 encoded text files."""

    def __init__(self, directory_manager: Optional[DirectoryManager] = None, *, encoding: str = 'utf-8'):
        self._directory_manager = directory_manager or DirectoryManager()
        self._encoding = encoding

    def ensure_parent(self, path: Path) -> None:
        self._directory_manager.ensure_parent(path)

    def write(self, path: Path, content: str) -> None:
        self.ensure_parent(path)
        path.write_text(content, encoding=self._encoding)


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


DEFAULT_DIRECTORY_MANAGER = DirectoryManager()
DEFAULT_TEXT_IO = TextFileIO(DEFAULT_DIRECTORY_MANAGER)


def write_text_file(path: Path, content: str) -> None:
    DEFAULT_TEXT_IO.write(path, content)
