"""Output helpers for the extractor pipeline."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, Optional, Sequence, Tuple

from extractor import io_utils

EntityRow = Tuple[str, str, str, str]


class EntitiesOutput:
    """Context manager wrapping TSV writing for entities."""

    HEADER = ['doc_id', 'type', 'value', 'offsets_json']

    def __init__(self, path: Path):
        self._writer = io_utils.TSVWriter(path, header=self.HEADER)
        self._active = False

    def __enter__(self) -> "EntitiesOutput":
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def open(self) -> None:
        """Open the underlying TSV writer."""
        if not self._active:
            self._writer.__enter__()
            self._active = True

    def close(self) -> None:
        """Close the underlying TSV writer."""
        if self._active:
            self._writer.__exit__(None, None, None)
            self._active = False

    def write(self, rows: Sequence[EntityRow]) -> None:
        """Write multiple entity rows to the TSV."""
        if not self._active:
            raise RuntimeError("EntitiesOutput must be opened before writing.")
        if rows:
            self._writer.write_rows(list(rows))

    def write_iterable(self, rows: Iterable[EntityRow]) -> None:
        """Write rows from an arbitrary iterable."""
        self.write(list(rows))


def _resolve_text_output_path(
    base_dir: Path,
    html_path: Path,
    doc_id: str,
    input_root: Path,
) -> Path:
    """Resolve the output path for a text artifact while mirroring directory structure."""
    relative = html_path.relative_to(input_root)
    return base_dir / relative.parent / f"{doc_id}.txt"


def text_exists(base_dir: Optional[Path], html_path: Path, doc_id: str, input_root: Path) -> bool:
    """Check whether the raw text file already exists."""
    if base_dir is None:
        return False
    return _resolve_text_output_path(base_dir, html_path, doc_id, input_root).exists()


def write_text(
    base_dir: Optional[Path],
    html_path: Path,
    doc_id: str,
    content: str,
    input_root: Path,
    *,
    force: bool = False,
) -> bool:
    """Write raw text content to disk.

    Returns:
        True if content was written, False otherwise.
    """
    if base_dir is None or not content:
        return False

    target_path = _resolve_text_output_path(base_dir, html_path, doc_id, input_root)
    if not force and target_path.exists():
        return False

    io_utils.write_text_file(target_path, content)
    return True
