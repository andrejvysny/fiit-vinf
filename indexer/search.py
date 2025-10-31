"""Utility classes for loading and querying the inverted index."""

from __future__ import annotations

import json
import math
from collections import Counter, defaultdict
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from .idf import IDFCalculator, IDFMethodError, IDFRegistry
from .tokenize import iter_tokens


@dataclass
class Document:
    doc_id: int
    title: str
    path: Path
    length: int
    tokenize_count: Optional[int] = None
    token_count: Optional[int] = None


@dataclass
class SearchResult:
    doc_id: int
    score: float
    title: str
    path: Path
    length: int
    tokenize_count: Optional[int]
    token_count: Optional[int]
    matched_terms: Dict[str, int]


@dataclass
class TermEntry:
    term: str
    df: int
    # postings maps doc_id -> list of token positions in that document
    postings: Dict[int, List[int]]
    idf: Dict[str, float] = field(default_factory=dict)

    def get_idf(self, method: str) -> float:
        return float(self.idf.get(method, 0.0))


@dataclass(frozen=True)
class IndexMetadata:
    total_docs: int
    total_terms: int
    # default method for display/selection: derive from available methods
    default_idf_method: str
    available_idf_methods: Tuple[str, ...]


@dataclass
class QueryVector:
    text: str
    tokens: List[str]
    term_freq: Counter[str]

    @classmethod
    def from_text(cls, text: str) -> "QueryVector":
        tokens = list(iter_tokens(text))
        return cls(text=text, tokens=tokens, term_freq=Counter(tokens))

    def is_empty(self) -> bool:
        return not self.tokens


class IndexRepository:
    """Filesystem-backed repository exposing manifest, documents, and postings."""

    def __init__(self, index_dir: Path, *, registry: Optional[IDFRegistry] = None) -> None:
        self.index_dir = Path(index_dir)
        if not self.index_dir.exists():
            raise FileNotFoundError(f"Index directory does not exist: {self.index_dir}")

        self.registry = registry or IDFRegistry()
        manifest_payload = self._load_manifest()
        metadata = self._parse_metadata(manifest_payload)
        self.documents = self._load_documents()
        self.terms = self._load_postings()
        metadata = self._with_counts(metadata)
        metadata = self._with_methods(metadata)
        self.metadata = metadata

    # ------------------------------------------------------------------ #
    # Loading helpers

    def _load_manifest(self) -> Dict:
        manifest_path = self.index_dir / "manifest.json"
        if not manifest_path.exists():
            raise FileNotFoundError(f"Manifest not found: {manifest_path}")
        return json.loads(manifest_path.read_text(encoding="utf-8"))

    def _parse_metadata(self, payload: Mapping[str, object]) -> IndexMetadata:
        raw_methods = payload.get("idf_methods")
        if raw_methods is None:
            available_methods = self.registry.supported_methods
        else:
            available_methods = self._parse_methods(raw_methods)

        # Choose a sensible display default: first available method (if any).
        if available_methods:
            default_method = available_methods[0]
        else:
            default_method = self.registry.ensure(None)

        total_docs = int(payload.get("total_docs") or 0)
        total_terms = int(payload.get("total_terms") or 0)

        return IndexMetadata(
            total_docs=total_docs,
            total_terms=total_terms,
            default_idf_method=default_method,
            available_idf_methods=available_methods,
        )

    def _parse_methods(self, raw_methods: object) -> Tuple[str, ...]:
        if raw_methods is None:
            return self.registry.supported_methods
        if isinstance(raw_methods, (list, tuple)):
            candidates = raw_methods
        else:
            candidates = [raw_methods]

        result: List[str] = []
        for method in candidates:
            canonical = self.registry.ensure(str(method))
            if canonical not in result:
                result.append(canonical)
        return tuple(result)

    def _load_documents(self) -> Dict[int, Document]:
        docs_path = self.index_dir / "docs.jsonl"
        if not docs_path.exists():
            raise FileNotFoundError(f"Document table not found: {docs_path}")

        documents: Dict[int, Document] = {}
        for line in docs_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            payload = json.loads(line)
            tokenize_count = payload.get("tokenize_count")
            if tokenize_count is not None:
                try:
                    tokenize_count = int(tokenize_count)
                except (TypeError, ValueError):
                    tokenize_count = None
            token_count = payload.get("tiktoken_token_count")
            if token_count is None:
                token_count = payload.get("token_count")
            doc = Document(
                doc_id=int(payload["doc_id"]),
                title=str(payload.get("title", "")),
                path=Path(payload.get("path", "")),
                length=int(payload.get("length", 0)),
                tokenize_count=tokenize_count,
                token_count=token_count,
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
            posting_map: Dict[int, List[int]] = {}
            for item in payload.get("postings", []):
                try:
                    doc_id = int(item.get("doc_id"))
                except Exception:
                    continue
                # Prefer explicit positions when available
                if "positions" in item and isinstance(item["positions"], list):
                    positions = [int(p) for p in item["positions"]]
                else:
                    # Fallback: if tf is provided, synthesize positions (0..tf-1)
                    tf = item.get("tf")
                    try:
                        tf_val = int(tf)
                    except Exception:
                        tf_val = 0
                    positions = list(range(tf_val))
                posting_map[doc_id] = positions
            idf_map = self._parse_idf_map(payload["term"], payload.get("idf"))
            entry = TermEntry(
                term=str(payload["term"]),
                df=int(payload.get("df", len(posting_map))),
                postings=posting_map,
                idf=idf_map,
            )
            terms[entry.term] = entry
        return terms

    def _parse_idf_map(self, term: object, raw_idf: object) -> Dict[str, float]:
        if not isinstance(raw_idf, dict):
            raise ValueError(f"Term {term!r} has invalid IDF table (expected dict, got {type(raw_idf).__name__}).")

        table: Dict[str, float] = {}
        for key, value in raw_idf.items():
            canonical = self.registry.ensure(str(key))
            try:
                table[canonical] = float(value)
            except (TypeError, ValueError) as exc:
                raise ValueError(f"Invalid IDF value for term {term!r} and method {canonical!r}") from exc

        expected = set(self.registry.supported_methods)
        if set(table.keys()) != expected:
            missing = expected - set(table.keys())
            raise ValueError(
                f"Term {term!r} does not contain IDF scores for all methods (missing: {sorted(missing)})"
            )
        return table

    def _with_counts(self, metadata: IndexMetadata) -> IndexMetadata:
        updated = metadata
        if metadata.total_docs <= 0:
            updated = replace(updated, total_docs=len(self.documents))
        if metadata.total_terms <= 0:
            updated = replace(updated, total_terms=len(self.terms))
        return updated

    def _with_methods(self, metadata: IndexMetadata) -> IndexMetadata:
        available = metadata.available_idf_methods
        expected = self.registry.supported_methods
        if set(available) != set(expected):
            raise ValueError(
                f"Index manifest is out of date. Expected IDF methods {expected}, found {available}."
            )
        return replace(metadata, available_idf_methods=expected)


class SearchEngine:
    """Coordinate query preparation, scoring, and ranking."""

    def __init__(self, index_dir: Path, *, registry: IDFRegistry | None = None) -> None:
        self.index_dir = Path(index_dir)
        self.registry = registry or IDFRegistry()
        self.repository = IndexRepository(self.index_dir, registry=self.registry)
        self.metadata = self.repository.metadata

        self.documents = self.repository.documents
        self.terms = self.repository.terms

        self.default_idf_method = self.metadata.default_idf_method
        self.available_idf_methods = self.metadata.available_idf_methods
        self.total_docs = self.metadata.total_docs

        self.idf_calculator = IDFCalculator(
            self.total_docs, registry=self.registry, methods=self.available_idf_methods
        )

    # ------------------------------------------------------------------ #
    # Public API

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
        query_vector = QueryVector.from_text(query)
        if query_vector.is_empty():
            return []

        method = self._select_method(idf_method)
        scores, matched_terms = self._score_documents(
            query_vector, method, use_stored_idf
        )
        if not scores:
            return []
        return self._rank_results(scores, matched_terms, top_k)

    # ------------------------------------------------------------------ #
    # Internal helpers

    def _select_method(self, method: Optional[str]) -> str:
        try:
            return self.registry.ensure(method or self.default_idf_method)
        except IDFMethodError as exc:
            raise ValueError(str(exc)) from exc

    def _score_documents(
        self,
        query: QueryVector,
        method: str,
        use_stored_idf: bool,
    ) -> Tuple[Dict[int, float], Dict[int, Dict[str, int]]]:
        doc_scores: Dict[int, float] = defaultdict(float)
        matched_terms: Dict[int, Dict[str, int]] = defaultdict(dict)

        for term, q_tf in query.term_freq.items():
            entry = self.terms.get(term)
            if entry is None or entry.df <= 0:
                continue

            idf_value = self._resolve_idf(entry, method, use_stored_idf)
            if idf_value <= 0.0:
                continue

            query_weight = 1.0 + math.log(q_tf)
            for doc_id, positions in entry.postings.items():
                tf = len(positions) if positions is not None else 0
                if tf <= 0:
                    continue
                tf_weight = 1.0 + math.log(tf)
                doc_scores[doc_id] += tf_weight * idf_value * query_weight
                matched_terms[doc_id][term] = tf

        return doc_scores, matched_terms

    def _resolve_idf(self, entry: TermEntry, method: str, use_stored: bool) -> float:
        # Prefer stored IDF values when available in the postings. The index
        # build step precomputes IDF scores for all supported methods and
        # stores them per-term; use those values to avoid runtime
        # recomputation and keep queries fast. Only fall back to computing
        # on-the-fly if the stored value is missing or zero.
        try:
            stored_value = entry.get_idf(method)
        except Exception:
            stored_value = 0.0

        if stored_value and stored_value != 0.0:
            return stored_value

        # If no stored value (or zero), fallback to compute. This path is
        # unlikely if the index was built with all supported methods.
        return self.idf_calculator.compute(entry.df, method)

    def _rank_results(
        self,
        scores: Mapping[int, float],
        matched_terms: Mapping[int, Dict[str, int]],
        top_k: int,
    ) -> List[SearchResult]:
        ranked = sorted(
            scores.items(), key=lambda item: (-item[1], item[0])
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
                    tokenize_count=doc.tokenize_count,
                    token_count=doc.token_count,
                    matched_terms=dict(sorted(matched_terms[doc_id].items())),
                )
            )
        return results


class InvertedIndex(SearchEngine):
    """Compatibility wrapper preserving the historical API name."""

    pass
