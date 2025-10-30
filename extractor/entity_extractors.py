"""Entity extraction functions using regex patterns.

This module contains functions to extract various entity types from HTML and text:
- GitHub metadata (stars, forks, language stats)
- README sections
- Licenses
- Topics
- Import statements
- URLs
- Issue references
- Versions
- Emails
- Code language hints
"""

import json
import re
from typing import Dict, List, Optional, Tuple

from extractor import html_clean, regexes


# Type alias for entity rows
EntityRow = Tuple[str, str, str, str]  # (doc_id, type, value, offsets_json)


def extract_star_count(doc_id: str, html_content: str) -> List[EntityRow]:
    """Extract star count from GitHub HTML.

    Args:
        doc_id: Document identifier
        html_content: Raw HTML string

    Returns:
        List of entity rows (doc_id, "STAR_COUNT", count_str, offsets_json)
    """
    results = []

    for pattern in regexes.get_star_regexes():
        matches = list(pattern.finditer(html_content))
        if not matches:
            continue

        # Take the first match
        match = matches[0]
        raw_count = match.group(1).strip()

        if not raw_count:
            continue

        # Store raw value (e.g., "34,222" or "34.2k")
        # Build offsets JSON
        offsets = [{
            'start': match.start(),
            'end': match.end(),
            'source': 'html'
        }]

        results.append((
            doc_id,
            'STAR_COUNT',
            raw_count,
            json.dumps(offsets, separators=(',', ':'))
        ))
        break  # Only take first successful match

    return results


def extract_fork_count(doc_id: str, html_content: str) -> List[EntityRow]:
    """Extract fork count from GitHub HTML.

    Args:
        doc_id: Document identifier
        html_content: Raw HTML string

    Returns:
        List of entity rows (doc_id, "FORK_COUNT", count_str, offsets_json)
    """
    results = []

    for pattern in regexes.get_fork_regexes():
        matches = list(pattern.finditer(html_content))
        if not matches:
            continue

        # Take the first match
        match = matches[0]
        raw_count = match.group(1).strip()

        if not raw_count:
            continue

        # Store raw value (e.g., "12,790" or "12.8k")
        # Build offsets JSON
        offsets = [{
            'start': match.start(),
            'end': match.end(),
            'source': 'html'
        }]

        results.append((
            doc_id,
            'FORK_COUNT',
            raw_count,
            json.dumps(offsets, separators=(',', ':'))
        ))
        break  # Only take first successful match

    return results


def extract_language_stats(doc_id: str, html_content: str) -> List[EntityRow]:
    """Extract language statistics from GitHub HTML.

    Tries to extract language names and percentages, building a JSON object.

    Args:
        doc_id: Document identifier
        html_content: Raw HTML string

    Returns:
        List of entity rows (doc_id, "LANG_STATS", json_str, offsets_json)
    """
    results = []
    patterns = regexes.get_lang_stats_regexes()

    # Find language names
    lang_names = []
    name_matches = list(patterns['name'].finditer(html_content))
    for match in name_matches:
        lang_names.append((match.group(1).strip(), match.start(), match.end()))

    if not lang_names:
        return results

    # Find all percentage values
    percent_values = []
    percent_matches = list(patterns['percent'].finditer(html_content))
    for match in percent_matches:
        try:
            val = float(match.group(1))
            percent_values.append((val, match.start(), match.end()))
        except ValueError:
            continue

    # Try to pair languages with percentages (assume they appear in proximity)
    # Simple heuristic: match languages with nearby percentages
    lang_stats = {}
    overall_start = None
    overall_end = None

    for lang_name, lang_start, lang_end in lang_names:
        if overall_start is None or lang_start < overall_start:
            overall_start = lang_start
        if overall_end is None or lang_end > overall_end:
            overall_end = lang_end

        # Find closest percentage after this language mention (within 500 chars)
        closest_percent = None
        closest_dist = float('inf')

        for percent, pct_start, pct_end in percent_values:
            if pct_start > lang_end and (pct_start - lang_end) < 500:
                dist = pct_start - lang_end
                if dist < closest_dist:
                    closest_dist = dist
                    closest_percent = percent
                    if pct_end > overall_end:
                        overall_end = pct_end

        if closest_percent is not None:
            lang_stats[lang_name] = closest_percent

    if lang_stats and overall_start is not None and overall_end is not None:
        # Create JSON value
        value_json = json.dumps(lang_stats, separators=(',', ':'), ensure_ascii=False)

        # Build offsets
        offsets = [{
            'start': overall_start,
            'end': overall_end,
            'source': 'html'
        }]

        results.append((
            doc_id,
            'LANG_STATS',
            value_json,
            json.dumps(offsets, separators=(',', ':'))
        ))

    return results


def extract_readme(doc_id: str, html_content: str) -> Tuple[Optional[str], List[EntityRow]]:
    """Extract README text from GitHub HTML.

    Returns both the README text (for writing to .readme.txt) and entity rows.

    Args:
        doc_id: Document identifier
        html_content: Raw HTML string

    Returns:
        Tuple of (readme_text or None, list of entity rows)
    """
    patterns = regexes.get_readme_regexes()
    results = []

    # Try article pattern first
    matches = list(patterns['article'].finditer(html_content))
    if matches:
        match = matches[0]
        readme_html = match.group(1)

        # Convert to plain text
        readme_text = html_clean.html_to_text(readme_html, strip_boilerplate=False)

        # Cap at 200k chars
        if len(readme_text) > 200000:
            readme_text = readme_text[:200000]

        if readme_text.strip():
            offsets = [{
                'start': match.start(),
                'end': match.end(),
                'source': 'html'
            }]

            results.append((
                doc_id,
                'README_SECTION',
                readme_text.strip(),
                json.dumps(offsets, separators=(',', ':'))
            ))

            return readme_text.strip(), results

    # Fallback: Try to find README in JSON payloads
    json_matches = list(patterns['json_script'].finditer(html_content))
    for json_match in json_matches:
        json_text = json_match.group(1)
        try:
            data = json.loads(json_text)
            # Search for readme-like keys
            readme_candidates = []

            def find_readme(obj, depth=0):
                if depth > 10:  # Prevent deep recursion
                    return
                if isinstance(obj, dict):
                    for key, val in obj.items():
                        if key.lower() in ('readme', 'richtext', 'markdown', 'text') and isinstance(val, str) and len(val) > 50:
                            readme_candidates.append(val)
                        else:
                            find_readme(val, depth + 1)
                elif isinstance(obj, list):
                    for item in obj:
                        find_readme(item, depth + 1)

            find_readme(data)

            if readme_candidates:
                # Take the longest candidate
                readme_html = max(readme_candidates, key=len)
                readme_text = html_clean.html_to_text(readme_html, strip_boilerplate=False)

                if len(readme_text) > 200000:
                    readme_text = readme_text[:200000]

                if readme_text.strip():
                    offsets = [{
                        'start': json_match.start(),
                        'end': json_match.end(),
                        'source': 'html'
                    }]

                    results.append((
                        doc_id,
                        'README_SECTION',
                        readme_text.strip(),
                        json.dumps(offsets, separators=(',', ':'))
                    ))

                    return readme_text.strip(), results

        except (json.JSONDecodeError, RecursionError):
            continue

    return None, results


def extract_licenses(doc_id: str, html_content: str, text_content: str) -> List[EntityRow]:
    """Extract license mentions from HTML and text.

    Args:
        doc_id: Document identifier
        html_content: Raw HTML string
        text_content: Preprocessed text string

    Returns:
        List of entity rows
    """
    results = []
    seen = set()  # Dedup by value

    for pattern in regexes.get_license_regexes():
        # Search in HTML
        for match in pattern.finditer(html_content):
            license_val = match.group(0).strip()
            if license_val in seen or not license_val:
                continue

            # Store raw value
            offsets = [{
                'start': match.start(),
                'end': match.end(),
                'source': 'html'
            }]

            results.append((
                doc_id,
                'LICENSE',
                license_val,
                json.dumps(offsets, separators=(',', ':'))
            ))
            seen.add(license_val)

        # Search in preprocessed text
        for match in pattern.finditer(text_content):
            license_val = match.group(0).strip()
            if license_val in seen or not license_val:
                continue

            # Store raw value
            offsets = [{
                'start': match.start(),
                'end': match.end(),
                'source': 'text'
            }]

            results.append((
                doc_id,
                'LICENSE',
                license_val,
                json.dumps(offsets, separators=(',', ':'))
            ))
            seen.add(license_val)

    return results


def extract_topics(doc_id: str, html_content: str) -> List[EntityRow]:
    """Extract repository topics/tags from GitHub HTML.

    Args:
        doc_id: Document identifier
        html_content: Raw HTML string

    Returns:
        List of entity rows
    """
    results = []
    seen = set()
    pattern = regexes.get_topic_regex()

    for match in pattern.finditer(html_content):
        topic = match.group(1).strip()
        if topic and topic not in seen:
            offsets = [{
                'start': match.start(),
                'end': match.end(),
                'source': 'html'
            }]

            results.append((
                doc_id,
                'TOPIC',
                topic,
                json.dumps(offsets, separators=(',', ':'))
            ))
            seen.add(topic)

    return results


def extract_imports(doc_id: str, text_content: str) -> List[EntityRow]:
    """Extract import statements from preprocessed text.

    Args:
        doc_id: Document identifier
        text_content: Preprocessed text string

    Returns:
        List of entity rows
    """
    results = []
    seen = set()
    import_patterns = regexes.get_import_regexes()

    for lang, pattern in import_patterns.items():
        for match in pattern.finditer(text_content):
            # Extract the imported module/package name
            import_val = None
            groups = match.groups()

            # Different patterns have different group structures
            for group in groups:
                if group:
                    import_val = group.strip()
                    break

            if import_val and import_val not in seen:
                offsets = [{
                    'start': match.start(),
                    'end': match.end(),
                    'source': 'text'
                }]

                results.append((
                    doc_id,
                    'IMPORT',
                    import_val,
                    json.dumps(offsets, separators=(',', ':'))
                ))
                seen.add(import_val)

    return results


def extract_urls(doc_id: str, html_content: str, text_content: str) -> List[EntityRow]:
    """Extract URLs from HTML and text.

    Args:
        doc_id: Document identifier
        html_content: Raw HTML string
        text_content: Preprocessed text string

    Returns:
        List of entity rows
    """
    results = []
    seen = set()
    url_patterns = regexes.get_url_regexes()

    # HTTP URLs from HTML
    for match in url_patterns['http'].finditer(html_content):
        url = match.group(0).strip()
        if url and url not in seen:
            # Store raw URL
            offsets = [{
                'start': match.start(),
                'end': match.end(),
                'source': 'html'
            }]

            results.append((
                doc_id,
                'URL',
                url,
                json.dumps(offsets, separators=(',', ':'))
            ))
            seen.add(url)

    # Markdown links from text
    for match in url_patterns['markdown'].finditer(text_content):
        url = match.group(2).strip()
        if url and url not in seen:
            # Store raw URL
            offsets = [{
                'start': match.start(),
                'end': match.end(),
                'source': 'text'
            }]

            results.append((
                doc_id,
                'URL',
                url,
                json.dumps(offsets, separators=(',', ':'))
            ))
            seen.add(url)

    return results


def extract_issue_refs(doc_id: str, text_content: str) -> List[EntityRow]:
    """Extract issue references from preprocessed text.

    Args:
        doc_id: Document identifier
        text_content: Preprocessed text string

    Returns:
        List of entity rows
    """
    results = []
    seen = set()

    for pattern in regexes.get_issue_ref_regexes():
        for match in pattern.finditer(text_content):
            ref_val = match.group(0).strip()
            if ref_val and ref_val not in seen:
                offsets = [{
                    'start': match.start(),
                    'end': match.end(),
                    'source': 'text'
                }]

                results.append((
                    doc_id,
                    'ISSUE_REF',
                    ref_val,
                    json.dumps(offsets, separators=(',', ':'))
                ))
                seen.add(ref_val)

    return results


def extract_versions(doc_id: str, text_content: str) -> List[EntityRow]:
    """Extract version strings from preprocessed text.

    Args:
        doc_id: Document identifier
        text_content: Preprocessed text string

    Returns:
        List of entity rows
    """
    results = []
    seen = set()
    pattern = regexes.get_version_regex()

    for match in pattern.finditer(text_content):
        version = match.group(1).strip()
        if version and version not in seen:
            # Store raw version (including leading 'v' if present)
            offsets = [{
                'start': match.start(),
                'end': match.end(),
                'source': 'text'
            }]

            results.append((
                doc_id,
                'VERSION',
                version,
                json.dumps(offsets, separators=(',', ':'))
            ))
            seen.add(version)

    return results


def extract_emails(doc_id: str, text_content: str) -> List[EntityRow]:
    """Extract email addresses from preprocessed text.

    Args:
        doc_id: Document identifier
        text_content: Preprocessed text string

    Returns:
        List of entity rows
    """
    results = []
    seen = set()
    pattern = regexes.get_email_regex()

    for match in pattern.finditer(text_content):
        email = match.group(1).strip()
        if email and email not in seen:
            # Store raw email
            offsets = [{
                'start': match.start(),
                'end': match.end(),
                'source': 'text'
            }]

            results.append((
                doc_id,
                'EMAIL',
                email,
                json.dumps(offsets, separators=(',', ':'))
            ))
            seen.add(email)

    return results


def extract_code_langs(doc_id: str, text_content: str) -> List[EntityRow]:
    """Extract code block language hints from preprocessed text.

    Args:
        doc_id: Document identifier
        text_content: Preprocessed text string

    Returns:
        List of entity rows
    """
    results = []
    seen = set()
    pattern = regexes.get_code_lang_regex()

    for match in pattern.finditer(text_content):
        lang = match.group(1).strip().lower()
        if lang and lang not in seen:
            offsets = [{
                'start': match.start(),
                'end': match.end(),
                'source': 'text'
            }]

            results.append((
                doc_id,
                'LANG',
                lang,
                json.dumps(offsets, separators=(',', ':'))
            ))
            seen.add(lang)

    return results


def extract_all_entities(doc_id: str, html_content: str, text_content: str) -> List[EntityRow]:
    """Extract all entity types from HTML and text.

    Orchestrates all extraction functions and returns combined results.

    Args:
        doc_id: Document identifier
        html_content: Raw HTML string
        text_content: Preprocessed text string

    Returns:
        List of all entity rows
    """
    results = []

    # GitHub metadata
    results.extend(extract_star_count(doc_id, html_content))
    results.extend(extract_fork_count(doc_id, html_content))
    results.extend(extract_language_stats(doc_id, html_content))

    # README (returns both text and entities)
    _, readme_entities = extract_readme(doc_id, html_content)
    results.extend(readme_entities)

    # Text-based entities
    results.extend(extract_licenses(doc_id, html_content, text_content))
    results.extend(extract_topics(doc_id, html_content))
    results.extend(extract_imports(doc_id, text_content))
    results.extend(extract_urls(doc_id, html_content, text_content))
    results.extend(extract_issue_refs(doc_id, text_content))
    results.extend(extract_versions(doc_id, text_content))
    results.extend(extract_emails(doc_id, text_content))
    results.extend(extract_code_langs(doc_id, text_content))

    return results
