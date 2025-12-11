"""Output helpers for text file writing."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from spark.lib.extractor import io_utils


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
