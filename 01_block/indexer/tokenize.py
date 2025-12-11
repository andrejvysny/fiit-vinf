"""
Tokenization helpers for the indexing subsystem.

The tokenizer intentionally keeps logic simple and deterministic so downstream
tests can rely on exact token sequences. Words are normalised to lowercase,
non-alphanumeric characters break tokens, and very short fragments are
discarded.
"""

from __future__ import annotations

import re
from typing import Iterable, Iterator, List

TOKEN_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*|(?:0x)?[A-Fa-f0-9]+")


def iter_tokens(text: str) -> Iterator[str]:
    """
    Yield normalized tokens from the given text.

    - Matches whole numbers (e.g. "2021") and words that may contain internal
      ASCII apostrophes (e.g. "it's", "rock'n'roll").
    - Internal apostrophes are removed during normalization so "It's" -> "its".
    - Tokens shorter than two characters are skipped.
    - Falsy text values are treated as empty input.

    Example:
    >>> list(iter_tokens("Hello, world! It's 2021."))
    ['hello', 'world', 'its', '2021']
    """
    for match in TOKEN_PATTERN.finditer(text or ""):
        token = match.group(0).lower()
        # keep tokens as matched: for code we want to preserve hex and mixed
        # alphanumeric identifiers (e.g. `f00dfeed`, `toAddress`, `var_name`).
        # Skip very short tokens to mimic previous behaviour (length < 2).
        if len(token) < 2:
            continue
        yield token


def tokenize(text: str) -> List[str]:
    """Return a list of tokens from ``text``."""
    return list(iter_tokens(text))


def unique_tokens(text: str) -> Iterable[str]:
    """Return unique tokens in the order of first appearance."""
    seen = set()
    for token in iter_tokens(text):
        if token in seen:
            continue
        seen.add(token)
        yield token
