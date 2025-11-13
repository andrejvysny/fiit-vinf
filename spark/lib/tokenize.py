"""Normalization and tokenization helpers for Spark UDFs."""

from __future__ import annotations

import re
import unicodedata
from typing import Iterable, Iterator, List, Tuple

CODE_FENCE_RE = re.compile(r"(```|~~~)[\s\S]*?\1", re.MULTILINE)
INLINE_CODE_RE = re.compile(r"`[^`]+`")
HTML_TAG_RE = re.compile(r"<[^>]+>")
WHITESPACE_RE = re.compile(r"\s+")
TOKEN_PATTERN = re.compile(r"[A-Za-z0-9_]+")


def normalize_text(text: str) -> str:
    """Lowercase, strip fences/tags, fold accents, and collapse whitespace."""
    if not text:
        return ""
    working = text.replace("\r\n", "\n").replace("\r", "\n")
    working = CODE_FENCE_RE.sub(" ", working)
    working = INLINE_CODE_RE.sub(" ", working)
    working = HTML_TAG_RE.sub(" ", working)
    working = unicodedata.normalize("NFKD", working)
    working = working.encode("ascii", "ignore").decode("ascii", errors="ignore")
    working = working.lower()
    working = WHITESPACE_RE.sub(" ", working)
    return working.strip()


def tokenize_with_positions(text: str) -> Tuple[List[str], List[int]]:
    """Return tokens plus start offsets in the normalized string."""
    tokens: List[str] = []
    positions: List[int] = []
    for match in TOKEN_PATTERN.finditer(text or ""):
        token = match.group(0)
        if len(token) < 2:
            continue
        tokens.append(token)
        positions.append(match.start())
    return tokens, positions


def iter_tokens(text: str) -> Iterator[str]:
    """Iterate over tokens only (helper for tests)."""
    normalized = normalize_text(text)
    return iter(tokenize_with_positions(normalized)[0])


def unique_tokens(text: str) -> Iterable[str]:
    """Yield unique tokens in order of appearance."""
    seen = set()
    for token in iter_tokens(text):
        if token in seen:
            continue
        seen.add(token)
        yield token


if __name__ == "__main__":
    sample = "Hello ```python\nprint('x')``` World!\n`inline` TEST"
    normalized = normalize_text(sample)
    assert normalized == "hello world test", normalized
    tokens, positions = tokenize_with_positions(normalized)
    assert tokens == ["hello", "world", "test"]
    assert positions == [0, 6, 12]
    print("tokenize.py self-check passed.")
