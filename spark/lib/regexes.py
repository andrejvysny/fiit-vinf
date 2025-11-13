"""Centralised regex definitions shared by Spark UDFs.

The patterns here mirror ``extractor.regexes`` but the more explosive
constructs were rewritten with reluctant qualifiers or character-class
boundaries to avoid catastrophic backtracking when executed inside
Spark workers.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Dict, Iterable, List, Pattern, Tuple

# ---------------------------------------------------------------------------
# Simple dataclasses to describe extraction recipes


@dataclass(frozen=True)
class RegexSpec:
    """Attach metadata to a compiled regex."""

    pattern: Pattern[str]
    value_group: int = 0


@dataclass(frozen=True)
class EntityMatch:
    """Represents a matched entity with offsets."""

    entity_type: str
    value: str
    start: int
    end: int


# ---------------------------------------------------------------------------
# Pattern definitions (roughly ordered by entity type)

STAR_REGEXES: Tuple[RegexSpec, ...] = (
    RegexSpec(
        re.compile(
            r'<span[^>]{0,200}?id="repo-stars-counter-star"[^>]*?\btitle="([0-9,]+)"',
            re.IGNORECASE,
        ),
        1,
    ),
    RegexSpec(
        re.compile(
            r'aria-label="([0-9,]+)\s+users?\s+starred',
            re.IGNORECASE,
        ),
        1,
    ),
)

FORK_REGEXES: Tuple[RegexSpec, ...] = (
    RegexSpec(
        re.compile(
            r'<span[^>]{0,200}?id="repo-network-counter"[^>]*?\btitle="([0-9,]+)"',
            re.IGNORECASE,
        ),
        1,
    ),
)

LANG_NAME_RE = re.compile(
    r'<span[^>]{0,200}?\bitemprop="programmingLanguage"[^>]*?>([^<]+)</span>',
    re.IGNORECASE,
)
LANG_PERCENT_RE = re.compile(r'(\d{1,3}(?:\.\d+)?)\s?%', re.MULTILINE)

LICENSE_REGEXES: Tuple[RegexSpec, ...] = (
    RegexSpec(
        re.compile(
            r'\b('
            r'MIT|Apache-2\.0|Apache-1\.1|GPL-3\.0|GPL-2\.0|LGPL-3\.0|LGPL-2\.1|'
            r'BSD-3-Clause|BSD-2-Clause|MPL-2\.0|ISC|CC0-1\.0|Unlicense|'
            r'AGPL-3\.0|EPL-2\.0|EPL-1\.0|CC-BY-4\.0|CC-BY-SA-4\.0'
            r')\b',
            re.IGNORECASE,
        ),
        1,
    ),
    RegexSpec(
        re.compile(
            r'<a[^>]{0,200}?href="[^"]*(?:license|LICENSE)[^"]*"[^>]*?>([^<]+)</a>',
            re.IGNORECASE,
        ),
        1,
    ),
)

TOPIC_REGEX = re.compile(
    r'<a[^>]{0,200}?(?:class="[^"]*?topic-tag[^"]*?"|data-octo-click="topic")[^>]*?>([\w\-]+)</a>',
    re.IGNORECASE,
)

URL_REGEXES: Tuple[RegexSpec, ...] = (
    RegexSpec(re.compile(r'https?://[^\s<>"\']+', re.IGNORECASE)),
    RegexSpec(re.compile(r'\[([^\]]+)\]\((https?://[^\s)]+)\)')),
    RegexSpec(re.compile(r'<a\s+[^>]*?href=["\']([^"\']+)["\']', re.IGNORECASE), 1),
)

EMAIL_REGEX = RegexSpec(
    re.compile(r'\b([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})\b'),
    1,
)

IMPORT_REGEXES: Dict[str, Pattern[str]] = {
    "python": re.compile(
        r'^\s*(?:import\s+([\w\.]+(?:\s*,\s*[\w\.]+)*)|from\s+([\w\.]+)\s+import\s+([\w\*]+(?:\s*,\s*[\w\*]+)*))',
        re.MULTILINE,
    ),
    "javascript": re.compile(
        r'^\s*(?:import\s+[^;\n]+?\s+from\s+[\'"]([^\'"]+)[\'"]|const\s+\w+\s*=\s*require\(\s*[\'"]([^\'"]+)[\'"]\s*\))',
        re.MULTILINE,
    ),
    "rust": re.compile(r'^\s*use\s+([A-Za-z0-9_:]+)', re.MULTILINE),
    "c": re.compile(r'^\s*#\s*include\s*[<"]([^>"]+)[>"]', re.MULTILINE),
    "php": re.compile(r'^\s*use\s+([\w\\]+)\s*;', re.MULTILINE),
    "go": re.compile(r'^\s*import\s+(?:"([^"]+)"|\((.*?)\))', re.MULTILINE | re.DOTALL),
}


# ---------------------------------------------------------------------------
# Helper utilities


def _iter_matches(specs: Iterable[RegexSpec], text: str) -> Iterable[Tuple[str, int, int]]:
    """Yield (value, start, end) tuples for the supplied specs."""
    for spec in specs:
        for match in spec.pattern.finditer(text):
            value = match.group(spec.value_group or 0)
            if not value:
                continue
            yield value.strip(), match.start(spec.value_group or 0), match.end(spec.value_group or 0)


def extract_topics(text: str) -> List[EntityMatch]:
    values = [match.group(1).strip() for match in TOPIC_REGEX.finditer(text) if match.group(1)]
    if not values:
        return []
    first = TOPIC_REGEX.search(text)
    last = None
    for last in TOPIC_REGEX.finditer(text):
        continue
    start = first.start(1) if first else 0
    end = last.end(1) if last else start
    return [EntityMatch("TOPICS", ",".join(values), start, end)]


def extract_language_stats(text: str) -> List[EntityMatch]:
    names = [(m.group(1).strip(), m.start(1), m.end(1)) for m in LANG_NAME_RE.finditer(text)]
    if not names:
        return []
    percents = [(float(m.group(1)), m.start(1), m.end(1)) for m in LANG_PERCENT_RE.finditer(text)]
    stats: Dict[str, float] = {}
    overall_start = None
    overall_end = None
    for name, start, end in names:
        if overall_start is None or start < overall_start:
            overall_start = start
        if overall_end is None or end > overall_end:
            overall_end = end
        nearest = None
        distance = 10_000.0
        for value, pct_start, pct_end in percents:
            if pct_start >= end and 0 <= (pct_start - end) < distance:
                nearest = value
                distance = pct_start - end
                if pct_end > (overall_end or pct_end):
                    overall_end = pct_end
        if nearest is not None:
            stats[name] = nearest
    if not stats:
        return []
    payload = ",".join(f"{k}:{v}" for k, v in stats.items())
    return [
        EntityMatch(
            "LANG_STATS",
            payload,
            overall_start or 0,
            overall_end or overall_start or 0,
        )
    ]


def extract_simple_entities(entity_type: str, specs: Iterable[RegexSpec], text: str) -> List[EntityMatch]:
    results: List[EntityMatch] = []
    for value, start, end in _iter_matches(specs, text):
        results.append(EntityMatch(entity_type, value, start, end))
    return results


def extract_urls(text: str) -> List[EntityMatch]:
    results: List[EntityMatch] = []
    seen = set()
    for spec in URL_REGEXES:
        for match in spec.pattern.finditer(text):
            value = match.group(spec.value_group or 0).strip()
            if not value or value in seen:
                continue
            seen.add(value)
            results.append(EntityMatch("URL", value, match.start(spec.value_group or 0), match.end(spec.value_group or 0)))
    return results


def extract_emails(text: str) -> List[EntityMatch]:
    return [
        EntityMatch("EMAIL", match.group(1).strip(), match.start(1), match.end(1))
        for match in EMAIL_REGEX.pattern.finditer(text)
    ]


def extract_imports(text: str) -> List[EntityMatch]:
    results: List[EntityMatch] = []
    for language, pattern in IMPORT_REGEXES.items():
        for match in pattern.finditer(text):
            groups = [grp for grp in match.groups() if grp]
            if not groups:
                continue
            results.append(
                EntityMatch(
                    "IMPORT",
                    f"{language}:{groups[0].strip()}",
                    match.start(),
                    match.end(),
                )
            )
    return results


def extract_all_entities(text: str) -> List[EntityMatch]:
    """Return all supported entities from ``text``."""
    entities: List[EntityMatch] = []
    entities.extend(extract_simple_entities("STAR_COUNT", STAR_REGEXES, text)[:1])
    entities.extend(extract_simple_entities("FORK_COUNT", FORK_REGEXES, text)[:1])
    entities.extend(extract_language_stats(text))
    entities.extend(extract_simple_entities("LICENSE", LICENSE_REGEXES, text))
    entities.extend(extract_topics(text))
    entities.extend(extract_urls(text))
    entities.extend(extract_emails(text))
    entities.extend(extract_imports(text))
    return entities
