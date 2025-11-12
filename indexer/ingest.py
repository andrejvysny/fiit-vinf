"""
Ingestion helpers for building the inverted index.

Responsible for scanning text files, turning them into token streams, and
emitting per-document statistics required by the storage writers.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, Iterator, List, Optional, Sequence

from .tokenize import tokenize


@dataclass
class DocumentRecord:
    """Metadata captured for each ingested document."""

    doc_id: int
    path: Path
    title: str
    tokens: List[str]
    term_freq: Dict[str, int]
    # Positions of each term in the document: term -> list of token indices
    term_positions: Dict[str, List[int]]
    token_count: Optional[int] = None

    @property
    def length(self) -> int:
        return len(self.tokens)


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return path.read_text(encoding="utf-8", errors="ignore")


def _guess_title(text: str, fallback: str) -> str:
    for line in text.splitlines():
        clean = line.strip()
        if len(clean) > 4:
            return clean[:256]
    return fallback


def iter_text_files(input_root: Path) -> Iterator[Path]:
    """Yield text files beneath ``input_root`` (sorted for determinism)."""
    candidates = sorted(
        p for p in input_root.rglob("*.txt") if p.is_file()
    )
    for path in candidates:
        yield path


"""The eager `load_documents` helper has been removed to enforce streaming-only indexing.

Use `iter_document_records(input_root, limit=None, token_counter=None)` to iterate
documents in a memory-efficient manner.
"""


def iter_document_records(
    input_root: Path,
    limit: Optional[int] = None,
    *,
    token_counter: Optional[Callable[[str], int]] = None,
) -> Iterator[DocumentRecord]:
    """Yield DocumentRecord objects one-by-one (streaming).

    This mirrors `load_documents` but avoids building the full list in memory.
    Doc IDs are assigned sequentially starting from 0.
    """
    for idx, path in enumerate(iter_text_files(input_root)):
        if limit is not None and idx >= limit:
            break

        text = _read_text(path)
        tokens = tokenize(text)
        frequencies = Counter(tokens)
        title = _guess_title(text, path.stem)

        token_count = token_counter(text) if token_counter is not None else None

        positions: Dict[str, List[int]] = {}
        for pos, tok in enumerate(tokens):
            bucket = positions.setdefault(tok, [])
            bucket.append(pos)

        record = DocumentRecord(
            doc_id=idx,
            path=path,
            title=title,
            tokens=tokens,
            term_freq=dict(frequencies),
            term_positions=positions,
            token_count=token_count,
        )
        yield record


def build_vocabulary(docs: Sequence[DocumentRecord]) -> Dict[str, Dict[int, int]]:
    """Construct postings from the loaded documents using term frequencies.

    Returns:
        Mapping term -> {doc_id: term_frequency}
    """
    postings: Dict[str, Dict[int, int]] = {}
    for doc in docs:
        for term, freq in doc.term_freq.items():
            bucket = postings.setdefault(term, {})
            bucket[doc.doc_id] = int(freq)
    return postings
