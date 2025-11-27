import json
from typing import List, Tuple

from extractor import regexes

EntityRow = Tuple[str, str, str, str]  # (doc_id, type, value, offsets_json)


def extract_star_count(doc_id: str, html_content: str) -> List[EntityRow]:
    """Extract repository star counts from GitHub HTML."""
    results: List[EntityRow] = []

    for pattern in regexes.get_star_regexes():
        matches = list(pattern.finditer(html_content))
        if not matches:
            continue

        match = matches[0]
        raw_count = match.group(1).strip()
        if not raw_count:
            continue

        offsets = [{
            'start': match.start(),
            'end': match.end(),
            'source': 'html',
        }]

        results.append((
            doc_id,
            'STAR_COUNT',
            raw_count,
            json.dumps(offsets, separators=(',', ':')),
        ))
        break

    return results


def extract_fork_count(doc_id: str, html_content: str) -> List[EntityRow]:
    """Extract fork counts from GitHub HTML."""
    results: List[EntityRow] = []

    for pattern in regexes.get_fork_regexes():
        matches = list(pattern.finditer(html_content))
        if not matches:
            continue

        match = matches[0]
        raw_count = match.group(1).strip()
        if not raw_count:
            continue

        offsets = [{
            'start': match.start(),
            'end': match.end(),
            'source': 'html',
        }]

        results.append((
            doc_id,
            'FORK_COUNT',
            raw_count,
            json.dumps(offsets, separators=(',', ':')),
        ))
        break

    return results


def extract_language_stats(doc_id: str, html_content: str) -> List[EntityRow]:
    """Extract language usage percentages from GitHub HTML."""
    results: List[EntityRow] = []
    patterns = regexes.get_lang_stats_regexes()

    lang_names = []
    name_matches = list(patterns['name'].finditer(html_content))
    for match in name_matches:
        lang_names.append((match.group(1).strip(), match.start(), match.end()))

    if not lang_names:
        return results

    percent_values = []
    percent_matches = list(patterns['percent'].finditer(html_content))
    for match in percent_matches:
        try:
            val = float(match.group(1))
            percent_values.append((val, match.start(), match.end()))
        except ValueError:
            continue

    lang_stats = {}
    overall_start = None
    overall_end = None

    for lang_name, lang_start, lang_end in lang_names:
        if overall_start is None or lang_start < overall_start:
            overall_start = lang_start
        if overall_end is None or lang_end > overall_end:
            overall_end = lang_end

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
        value_json = json.dumps(lang_stats, separators=(',', ':'), ensure_ascii=False)
        offsets = [{
            'start': overall_start,
            'end': overall_end,
            'source': 'html',
        }]

        results.append((
            doc_id,
            'LANG_STATS',
            value_json,
            json.dumps(offsets, separators=(',', ':')),
        ))

    return results


def extract_urls(doc_id: str, html_content: str) -> List[EntityRow]:
    """Extract URLs from HTML (hrefs, raw http(s) strings, markdown links)."""
    results: List[EntityRow] = []
    url_regexes = regexes.get_url_regexes()
    seen = set()

    # href attributes
    for m in url_regexes['html'].finditer(html_content):
        url = (m.group(1) or '').strip()
        if not url:
            continue
        if url in seen:
            continue
        offsets = [{'start': m.start(1), 'end': m.end(1), 'source': 'html'}]
        results.append((doc_id, 'URL', url, json.dumps(offsets, separators=(',', ':'))))
        seen.add(url)

    # explicit http(s) strings
    for m in url_regexes['http'].finditer(html_content):
        url = m.group(0).strip()
        if not url or url in seen:
            continue
        offsets = [{'start': m.start(), 'end': m.end(), 'source': 'html'}]
        results.append((doc_id, 'URL', url, json.dumps(offsets, separators=(',', ':'))))
        seen.add(url)

    # markdown-style links (text inside HTML can contain them)
    for m in url_regexes['markdown'].finditer(html_content):
        url = (m.group(2) or '').strip()
        if not url or url in seen:
            continue
        offsets = [{'start': m.start(2), 'end': m.end(2), 'source': 'html'}]
        results.append((doc_id, 'URL', url, json.dumps(offsets, separators=(',', ':'))))
        seen.add(url)

    return results


def extract_readme(doc_id: str, html_content: str) -> List[EntityRow]:
    """Attempt to find README content embedded in the page (article or JSON)."""
    results: List[EntityRow] = []
    patterns = regexes.get_readme_regexes()

    # Try article container first
    m = patterns['article'].search(html_content)
    if m:
        snippet = m.group(1).strip()
        if snippet:
            offsets = [{'start': m.start(1), 'end': m.end(1), 'source': 'html'}]
            results.append((doc_id, 'README', snippet, json.dumps(offsets, separators=(',', ':'))))
            return results

    # Fallback: try script JSON payloads and look for fields that may contain README
    for m in patterns['json_script'].finditer(html_content):
        raw = m.group(1).strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            # not a JSON payload we can parse
            continue

        # Common keys used by GitHub JSON blobs
        for key in ('readme', 'richText', 'body', 'content'):
            if key in data and isinstance(data[key], (str,)) and data[key].strip():
                val = data[key].strip()
                offsets = [{'start': m.start(1), 'end': m.end(1), 'source': 'html'}]
                results.append((doc_id, 'README', val, json.dumps(offsets, separators=(',', ':'))))
                return results

        # Nested fields: search string values recursively (shallow)
        for v in data.values() if isinstance(data, dict) else []:
            if isinstance(v, str) and len(v) > 50:
                offsets = [{'start': m.start(1), 'end': m.end(1), 'source': 'html'}]
                results.append((doc_id, 'README', v.strip(), json.dumps(offsets, separators=(',', ':'))))
                return results

    return results


def extract_license(doc_id: str, html_content: str) -> List[EntityRow]:
    """Find license identifiers or license links."""
    results: List[EntityRow] = []
    for pattern in regexes.get_license_regexes():
        for m in pattern.finditer(html_content):
            val = (m.group(0) or '').strip()
            if not val:
                continue
            offsets = [{'start': m.start(), 'end': m.end(), 'source': 'html'}]
            results.append((doc_id, 'LICENSE', val, json.dumps(offsets, separators=(',', ':'))))
    return results


def extract_topics(doc_id: str, html_content: str) -> List[EntityRow]:
    """Extract repository topic tags from the HTML."""
    results: List[EntityRow] = []
    pattern = regexes.get_topic_regex()
    topics = [m.group(1).strip() for m in pattern.finditer(html_content) if m.group(1)]
    if topics:
        # store as comma-separated list
        val = ','.join(topics)
        # approximate offsets from first to last match
        first = pattern.search(html_content)
        last = None
        for m in pattern.finditer(html_content):
            last = m
        if first and last:
            offsets = [{'start': first.start(1), 'end': last.end(1), 'source': 'html'}]
        else:
            offsets = [{'start': 0, 'end': 0, 'source': 'html'}]
        results.append((doc_id, 'TOPICS', val, json.dumps(offsets, separators=(',', ':'))))
    return results


def extract_emails(doc_id: str, html_content: str) -> List[EntityRow]:
    """Find email addresses in HTML/text."""
    results: List[EntityRow] = []
    pattern = regexes.get_email_regex()
    for m in pattern.finditer(html_content):
        val = (m.group(1) or '').strip()
        if not val:
            continue
        offsets = [{'start': m.start(1), 'end': m.end(1), 'source': 'html'}]
        results.append((doc_id, 'EMAIL', val, json.dumps(offsets, separators=(',', ':'))))
    return results


def extract_all_entities(doc_id: str, html_content: str) -> List[EntityRow]:
    """Extract supported entity types from GitHub HTML."""
    results: List[EntityRow] = []
    # Basic repo signals
    results.extend(extract_star_count(doc_id, html_content))
    results.extend(extract_fork_count(doc_id, html_content))
    results.extend(extract_language_stats(doc_id, html_content))

    # Secondary entities
    results.extend(extract_readme(doc_id, html_content))
    results.extend(extract_license(doc_id, html_content))
    results.extend(extract_topics(doc_id, html_content))
    results.extend(extract_urls(doc_id, html_content))
    results.extend(extract_emails(doc_id, html_content))
    return results
