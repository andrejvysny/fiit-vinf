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
from typing import Dict, List, Mapping, Sequence

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


def write_postings(
    output_dir: Path,
    vocabulary: Mapping[str, Mapping[int, int]],
    idf_tables: Mapping[str, Mapping[str, float]],
) -> None:
    path = output_dir / "postings.jsonl"
    lines: List[str] = []
    for term in sorted(vocabulary.keys()):
        postings = [
            {"doc_id": doc_id, "tf": tf}
            for doc_id, tf in sorted(vocabulary[term].items())
        ]
        idf_payload: Dict[str, float] = {}
        for method in sorted(idf_tables.keys()):
            idf_payload[method] = float(idf_tables[method].get(term, 0.0))
        payload = {
            "term": term,
            "df": len(postings),
            "idf": idf_payload,
            "postings": postings,
        }
        lines.append(json.dumps(payload, ensure_ascii=False))
    _atomic_write(path, "\n".join(lines) + ("\n" if lines else ""))


def write_manifest(
    output_dir: Path,
    *,
    total_docs: int,
    total_terms: int,
    idf_method: str,
    idf_methods: Sequence[str] | None = None,
) -> None:
    path = output_dir / "manifest.json"
    payload = {
        "total_docs": total_docs,
        "total_terms": total_terms,
        "idf_method": idf_method,
    }
    if idf_methods is not None:
        payload["idf_methods"] = list(idf_methods)
    _atomic_write(path, json.dumps(payload, indent=2, ensure_ascii=False) + "\n")
