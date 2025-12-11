"""IDF computation utilities shared across the indexing stack."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, Iterable, Mapping, Tuple

DEFAULT_IDF_METHOD = "classic"

# Canonical methods required by the refactor.
CANONICAL_IDF_METHODS: Tuple[str, ...] = (
    "classic",
    "smoothed",
    "probabilistic",
    "max",
)


class IDFMethodError(ValueError):
    """Raised when an unsupported IDF strategy is requested."""


@dataclass(frozen=True)
class IDFRegistry:
    """Registry responsible for normalising and validating IDF method names."""

    supported_methods: Tuple[str, ...] = CANONICAL_IDF_METHODS

    def __post_init__(self) -> None:
        lookup = {method: method for method in self.supported_methods}
        object.__setattr__(self, "_lookup", lookup)

    @property
    def default(self) -> str:
        return DEFAULT_IDF_METHOD

    def ensure(self, method: str | None) -> str:
        candidate = (method or self.default).strip().lower()
        if not candidate:
            candidate = self.default
        if candidate not in self._lookup:
            raise IDFMethodError(f"Unsupported IDF method: {candidate!r}")
        return candidate

    def ensure_many(self, methods: Iterable[str] | None) -> Tuple[str, ...]:
        if methods is None:
            return self.supported_methods
        seen = []
        for method in methods:
            canonical = self.ensure(method)
            if canonical not in seen:
                seen.append(canonical)
        return tuple(seen)


class IDFCalculator:
    """Compute inverse document frequency scores for the configured methods."""

    def __init__(
        self,
        total_docs: int,
        *,
        registry: IDFRegistry | None = None,
        methods: Iterable[str] | None = None,
    ) -> None:
        self.total_docs = max(int(total_docs), 0)
        self.registry = registry or IDFRegistry()
        self.methods = self.registry.ensure_many(methods)

    # ------------------------------------------------------------------ #
    # Public API

    def compute(self, df: int, method: str | None = None) -> float:
        canonical = self.registry.ensure(method)
        return self._compute_single(df, canonical)

    def compute_all(
        self, df: int, methods: Iterable[str] | None = None
    ) -> Dict[str, float]:
        selected = self.methods if methods is None else self.registry.ensure_many(methods)
        return {method: self._compute_single(df, method) for method in selected}

    def compute_tables(
        self,
        vocabulary: Mapping[str, Mapping[int, int]],
        methods: Iterable[str] | None = None,
    ) -> Dict[str, Dict[str, float]]:
        selected_methods = (
            self.methods if methods is None else self.registry.ensure_many(methods)
        )
        tables: Dict[str, Dict[str, float]] = {method: {} for method in selected_methods}
        for term, postings in vocabulary.items():
            df = len(postings)
            scores = self.compute_all(df, selected_methods)
            for method, value in scores.items():
                tables[method][term] = value
        return tables

    # ------------------------------------------------------------------ #
    # Internal helpers

    def _compute_single(self, df: int, method: str) -> float:
        if self.total_docs <= 0 or df <= 0:
            return 0.0

        if method == "classic":
            return self._classic(df)
        if method == "smoothed":
            return self._smoothed(df)
        if method == "probabilistic":
            return self._probabilistic(df)
        if method == "max":
            return self._max(df)
        raise IDFMethodError(f"Unsupported IDF method: {method}")

    def _classic(self, df: int) -> float:
        ratio = self.total_docs / max(df, 1)
        if ratio <= 0.0:
            return 0.0
        return math.log(ratio)

    def _smoothed(self, df: int) -> float:
        numerator = self.total_docs + 1.0
        denominator = df + 1.0
        if denominator <= 0.0:
            denominator = 1.0
        ratio = numerator / denominator
        if ratio <= 0.0:
            return 0.0
        return math.log(ratio) + 1.0

    def _probabilistic(self, df: int) -> float:
        numerator = self.total_docs - df + 0.5
        denominator = df + 0.5
        if denominator <= 0.0:
            denominator = 1.0
        ratio = numerator / denominator
        value = ratio + 1.0
        if value <= 0.0:
            return 0.0
        return math.log(value)

    def _max(self, df: int) -> float:
        if self.total_docs <= 1:
            return 0.0
        ratio = self.total_docs / max(df, 1)
        if ratio <= 0.0:
            return 0.0
        denominator = math.log(self.total_docs)
        if denominator == 0.0:
            return 0.0
        return math.log(ratio) / denominator


SUPPORTED_IDF_METHODS = IDFRegistry().supported_methods
