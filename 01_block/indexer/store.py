"""
Persistence helpers for the inverted index.

Outputs JSONL artefacts for ease of inspection and reproducibility. Each file
is written atomically by first dumping to a temporary path and then renaming
into place.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict, List, Mapping

from .ingest import DocumentRecord


def _atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(content, encoding="utf-8")
    os.replace(tmp_path, path)


def write_docs(output_dir: Path, docs: Sequence[DocumentRecord]) -> None:
    path = output_dir / "docs.jsonl"
    lines = []
    for record in docs:
        payload = {
            "doc_id": record.doc_id,
            "path": str(record.path),
            "title": record.title,
            "length": record.length,
        }
        payload["tokenize_count"] = record.length
        if record.token_count is not None:
            payload["tiktoken_token_count"] = record.token_count
        lines.append(json.dumps(payload, ensure_ascii=False))
    _atomic_write(path, "\n".join(lines) + ("\n" if lines else ""))


def append_docs(output_dir: Path, docs: Sequence[DocumentRecord]) -> None:
    """Append document metadata lines to docs.jsonl (non-atomic append).

    Used by chunked indexing to avoid holding all documents in memory.
    """
    path = output_dir / "docs.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        for record in docs:
            payload = {
                "doc_id": record.doc_id,
                "path": str(record.path),
                "title": record.title,
                "length": record.length,
            }
            payload["tokenize_count"] = record.length
            if record.token_count is not None:
                payload["tiktoken_token_count"] = record.token_count
            fh.write(json.dumps(payload, ensure_ascii=False))
            fh.write("\n")


def write_postings(
    output_dir: Path,
    vocabulary: Mapping[str, Mapping[int, int]],
    idf_tables: Mapping[str, Mapping[str, float]],
) -> None:
    postings_path = output_dir / "postings.jsonl"
    index_path = output_dir / "terms.idx"
    postings_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_postings = postings_path.with_name(postings_path.name + ".tmp")
    tmp_index = index_path.with_name(index_path.name + ".tmp")
    with tmp_postings.open("wb") as postings_fh, tmp_index.open("w", encoding="utf-8") as index_fh:
        for term in sorted(vocabulary.keys()):
            postings = []
            for doc_id, tf in sorted(vocabulary[term].items()):
                postings.append({"doc_id": doc_id, "tf": int(tf)})
            idf_payload: Dict[str, float] = {}
            for method in sorted(idf_tables.keys()):
                idf_payload[method] = float(idf_tables[method].get(term, 0.0))
            payload = {
                "term": term,
                "df": len(postings),
                "idf": idf_payload,
                "postings": postings,
            }
            line = json.dumps(payload, ensure_ascii=False)
            encoded = (line + "\n").encode("utf-8")
            offset = postings_fh.tell()
            postings_fh.write(encoded)
            index_entry = {
                "term": term,
                "offset": int(offset),
                "length": int(len(encoded)),
            }
            index_fh.write(json.dumps(index_entry, ensure_ascii=False))
            index_fh.write("\n")
    os.replace(tmp_postings, postings_path)
    os.replace(tmp_index, index_path)


def write_partial_postings(path: Path, vocabulary: Mapping[str, Mapping[int, int]]) -> None:
    """Write a partial postings file for a chunk. Each line contains term and postings only.

    The caller is responsible for writing into a partial directory. This format is
    intentionally lightweight to support external k-way merge later.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    lines: List[str] = []
    for term in sorted(vocabulary.keys()):
        postings = [
            {"doc_id": doc_id, "tf": int(tf)}
            for doc_id, tf in sorted(vocabulary[term].items())
        ]
        payload = {"term": term, "postings": postings}
        lines.append(json.dumps(payload, ensure_ascii=False))
    path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")


def write_manifest(
    output_dir: Path,
    *,
    total_docs: int,
    total_terms: int,
    idf_methods: Sequence[str] | None = None,
) -> None:
    path = output_dir / "manifest.json"
    payload = {
        "total_docs": total_docs,
        "total_terms": total_terms,
    }
    if idf_methods is not None:
        payload["idf_methods"] = list(idf_methods)
    _atomic_write(path, json.dumps(payload, indent=2, ensure_ascii=False) + "\n")
