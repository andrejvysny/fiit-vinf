"""Utility classes for loading and querying the inverted index."""

from __future__ import annotations

import json
import math
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from .build import SUPPORTED_IDF_METHODS, compute_idf
from .tokenize import iter_tokens


@dataclass
class Document:
    doc_id: int
    title: str
    path: Path
    length: int
    token_count: Optional[int] = None


@dataclass
class SearchResult:
    doc_id: int
    score: float
    title: str
    path: Path
    length: int
    token_count: Optional[int]
    matched_terms: Dict[str, int]


@dataclass
class TermEntry:
    term: str
    df: int
    postings: Dict[int, int]
    idf: Dict[str, float] = field(default_factory=dict)

    def get_idf(self, method: str) -> float:
        return float(self.idf.get(method, 0.0))


class InvertedIndex:
    """In-memory representation of the generated index."""

    def __init__(self, index_dir: Path) -> None:
        self.index_dir = index_dir
        self.manifest = self._load_manifest()
        self.default_idf_method = str(self.manifest.get("idf_method", "log"))
        self.available_idf_methods = tuple(
            self.manifest.get("idf_methods", sorted(SUPPORTED_IDF_METHODS))
        )
        self.documents = self._load_documents()
        self.terms = self._load_postings()
        self.total_docs = int(self.manifest.get("total_docs", len(self.documents)))

    # ------------------------------------------------------------------
    # Loading helpers

    def _load_manifest(self) -> Dict:
        manifest_path = self.index_dir / "manifest.json"
        if not manifest_path.exists():
            raise FileNotFoundError(f"Manifest not found: {manifest_path}")
        return json.loads(manifest_path.read_text(encoding="utf-8"))

    def _load_documents(self) -> Dict[int, Document]:
        docs_path = self.index_dir / "docs.jsonl"
        if not docs_path.exists():
            raise FileNotFoundError(f"Document table not found: {docs_path}")

        documents: Dict[int, Document] = {}
        for line in docs_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            payload = json.loads(line)
            doc = Document(
                doc_id=int(payload["doc_id"]),
                title=str(payload.get("title", "")),
                path=Path(payload.get("path", "")),
                length=int(payload.get("length", 0)),
                token_count=payload.get("token_count"),
            )
            documents[doc.doc_id] = doc
        return documents

    def _load_postings(self) -> Dict[str, TermEntry]:
        postings_path = self.index_dir / "postings.jsonl"
        if not postings_path.exists():
            raise FileNotFoundError(f"Postings file not found: {postings_path}")

        terms: Dict[str, TermEntry] = {}
        for line in postings_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            payload = json.loads(line)
            posting_map = {
                int(item["doc_id"]): int(item["tf"])
                for item in payload.get("postings", [])
            }
            raw_idf = payload.get("idf", {})
            if isinstance(raw_idf, dict):
                idf_map = {str(key): float(value) for key, value in raw_idf.items()}
            else:
                # Backwards compatibility with indexes storing a single float.
                idf_map = {self.default_idf_method: float(raw_idf)}

            entry = TermEntry(
                term=str(payload["term"]),
                df=int(payload.get("df", len(posting_map))),
                postings=posting_map,
                idf=idf_map,
            )
            terms[entry.term] = entry
        return terms

    # ------------------------------------------------------------------
    # Querying

    def available_terms(self) -> Iterable[str]:
        return self.terms.keys()

    def search(
        self,
        query: str,
        *,
        top_k: int = 10,
        idf_method: Optional[str] = None,
        use_stored_idf: bool = False,
    ) -> List[SearchResult]:
        if not query:
            return []

        query_tokens = list(iter_tokens(query))
        if not query_tokens:
            return []

        selected_method = idf_method or self.default_idf_method
        if selected_method not in SUPPORTED_IDF_METHODS:
            raise ValueError(
                f"Unsupported idf_method: {selected_method}. Supported: {sorted(SUPPORTED_IDF_METHODS)}"
            )

        query_tf = Counter(query_tokens)
        doc_scores: Dict[int, float] = defaultdict(float)
        matched_terms: Dict[int, Dict[str, int]] = defaultdict(dict)

        for term, q_tf in query_tf.items():
            entry = self.terms.get(term)
            if entry is None or entry.df == 0:
                continue

            if use_stored_idf:
                idf_value = entry.get_idf(selected_method)
                if idf_value == 0.0:
                    # Fall back to on-the-fly computation if the stored table lacks the term.
                    idf_value = compute_idf(entry.df, self.total_docs, selected_method)
            else:
                idf_value = compute_idf(entry.df, self.total_docs, selected_method)

            query_weight = 1.0 + math.log(q_tf)

            for doc_id, tf in entry.postings.items():
                if tf <= 0:
                    continue
                tf_weight = 1.0 + math.log(tf)
                doc_scores[doc_id] += tf_weight * idf_value * query_weight
                matched_terms[doc_id][term] = tf

        if not doc_scores:
            return []

        ranked = sorted(
            doc_scores.items(), key=lambda item: (-item[1], item[0])
        )[: max(top_k, 0)]

        results: List[SearchResult] = []
        for doc_id, score in ranked:
            doc = self.documents.get(doc_id)
            if doc is None:
                continue
            results.append(
                SearchResult(
                    doc_id=doc_id,
                    score=score,
                    title=doc.title,
                    path=doc.path,
                    length=doc.length,
                    token_count=doc.token_count,
                    matched_terms=dict(sorted(matched_terms[doc_id].items())),
                )
            )

        return results
