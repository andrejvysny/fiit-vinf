"""
Regex patterns and utilities for MediaWiki XML parsing.

Based on MediaWiki XML export format documentation:
https://www.mediawiki.org/wiki/Help:Export#Export_format
"""

import re
import unicodedata
from typing import Dict, List, Optional, Tuple

# Core XML field patterns (bounded, non-greedy)
TITLE_PATTERN = re.compile(r'<title>(.*?)</title>', re.DOTALL)
PAGE_ID_PATTERN = re.compile(r'<id>(\d+)</id>')  # First occurrence in page scope
NAMESPACE_PATTERN = re.compile(r'<ns>(\d+)</ns>')
REDIRECT_PATTERN = re.compile(r'<redirect\s+title="([^"]+)"')
TIMESTAMP_PATTERN = re.compile(r'<timestamp>(.*?)</timestamp>')
TEXT_PATTERN = re.compile(r'<text[^>]*>(.*?)</text>', re.DOTALL)

# Within wikitext patterns
CATEGORY_PATTERN = re.compile(r'\[\[Category:([^\]|]+)(?:\|[^\]]+)?\]\]', re.IGNORECASE)
INTERNAL_LINK_PATTERN = re.compile(r'\[\[([^\]|#]+)(?:[#\|][^\]]+)?\]\]')
EXTERNAL_LINK_PATTERN = re.compile(r'\[https?://[^\s\]]+[^\]]*\]')

# Infobox pattern (simplified)
INFOBOX_START_PATTERN = re.compile(r'\{\{Infobox[^\n]*', re.IGNORECASE)
INFOBOX_FIELD_PATTERN = re.compile(r'^\s*\|\s*([^=]+?)\s*=\s*(.+?)(?=\n\s*\||$)', re.MULTILINE)

# Section header pattern
SECTION_PATTERN = re.compile(r'^={2,}[^=]+=+', re.MULTILINE)


def extract_page_xml(page_xml: str) -> Optional[Dict[str, any]]:
    """Extract core fields from a <page> XML block."""
    # Title (required)
    title_match = TITLE_PATTERN.search(page_xml)
    if not title_match:
        return None
    title = title_match.group(1).strip()

    # Page ID (required)
    id_match = PAGE_ID_PATTERN.search(page_xml)
    if not id_match:
        return None
    page_id = int(id_match.group(1))

    # Namespace (default to 0)
    ns_match = NAMESPACE_PATTERN.search(page_xml)
    namespace = int(ns_match.group(1)) if ns_match else 0

    # Redirect (optional)
    redirect_match = REDIRECT_PATTERN.search(page_xml)
    redirect_to = redirect_match.group(1) if redirect_match else None

    # Timestamp (optional)
    timestamp_match = TIMESTAMP_PATTERN.search(page_xml)
    timestamp = timestamp_match.group(1) if timestamp_match else None

    # Text content (optional but usually present)
    text_match = TEXT_PATTERN.search(page_xml)
    text = text_match.group(1) if text_match else ""

    # Clean CDATA if present
    if text.startswith("<![CDATA[") and text.endswith("]]>"):
        text = text[9:-3]

    return {
        'page_id': page_id,
        'title': title,
        'namespace': namespace,
        'redirect_to': redirect_to,
        'timestamp': timestamp,
        'text': text
    }


def normalize_title(title: str) -> str:
    """
    Normalize a title for matching:
    - Lowercase
    - ASCII-fold (remove accents)
    - Collapse spaces and punctuation
    - Strip parenthetical suffixes
    """
    if not title:
        return ""

    # Remove parenthetical suffixes like "(disambiguation)" or "(programming language)"
    title = re.sub(r'\s*\([^)]*\)\s*$', '', title)

    # Convert to lowercase
    title = title.lower()

    # ASCII-fold (remove accents/diacritics)
    title = ''.join(
        c for c in unicodedata.normalize('NFD', title)
        if unicodedata.category(c) != 'Mn'
    )

    # Replace punctuation and spaces with single space
    title = re.sub(r'[^\w\s]+', ' ', title)
    title = re.sub(r'\s+', ' ', title)

    return title.strip()


def extract_categories(text: str) -> List[str]:
    """Extract category names from wikitext."""
    if not text:
        return []

    categories = []
    for match in CATEGORY_PATTERN.finditer(text):
        category = match.group(1).strip()
        if category:
            categories.append(category)

    return categories


def extract_internal_links(text: str) -> List[str]:
    """Extract internal link targets from wikitext."""
    if not text:
        return []

    links = []
    for match in INTERNAL_LINK_PATTERN.finditer(text):
        link = match.group(1).strip()
        # Filter out File:, Image:, Category: links
        if not any(link.startswith(prefix) for prefix in ['File:', 'Image:', 'Category:']):
            links.append(link)

    return links


def extract_infobox_fields(text: str, max_fields: int = 20) -> Dict[str, str]:
    """
    Extract key-value pairs from the first infobox.
    Limited to max_fields for safety.
    """
    if not text:
        return {}

    # Find the start of an infobox
    infobox_start = INFOBOX_START_PATTERN.search(text)
    if not infobox_start:
        return {}

    # Extract text from infobox start to the end (simplified - looks for }})
    start_pos = infobox_start.end()
    # Find the closing }} (handling nested templates roughly)
    brace_count = 2
    end_pos = start_pos
    for i in range(start_pos, min(start_pos + 10000, len(text))):  # Limit search
        if text[i:i+2] == '{{':
            brace_count += 2
            i += 1
        elif text[i:i+2] == '}}':
            brace_count -= 2
            if brace_count == 0:
                end_pos = i
                break
            i += 1

    if end_pos <= start_pos:
        return {}

    infobox_text = text[start_pos:end_pos]

    # Extract fields
    fields = {}
    for match in INFOBOX_FIELD_PATTERN.finditer(infobox_text):
        if len(fields) >= max_fields:
            break
        key = match.group(1).strip()
        value = match.group(2).strip()
        # Clean up wiki markup from values (basic)
        value = re.sub(r'\[\[([^|\]]+)\|([^\]]+)\]\]', r'\2', value)  # [[link|text]] -> text
        value = re.sub(r'\[\[([^\]]+)\]\]', r'\1', value)  # [[link]] -> link
        value = re.sub(r"'''?", '', value)  # Remove bold/italic
        fields[key] = value

    return fields


def extract_abstract(text: str, max_length: int = 1000) -> str:
    """
    Extract the abstract/lead section (text before first heading or blank line).
    """
    if not text:
        return ""

    # Remove redirect markup
    if text.strip().startswith("#REDIRECT"):
        return ""

    # Find first section heading
    section_match = SECTION_PATTERN.search(text)
    if section_match:
        text = text[:section_match.start()]

    # Find first paragraph (text before double newline)
    paragraphs = text.split('\n\n')
    if paragraphs:
        abstract = paragraphs[0].strip()

        # Clean up wiki markup
        # Remove templates {{ }}
        abstract = re.sub(r'\{\{[^}]+\}\}', '', abstract)
        # Convert links to plain text
        abstract = re.sub(r'\[\[([^|\]]+)\|([^\]]+)\]\]', r'\2', abstract)
        abstract = re.sub(r'\[\[([^\]]+)\]\]', r'\1', abstract)
        # Remove references <ref>...</ref>
        abstract = re.sub(r'<ref[^>]*>.*?</ref>', '', abstract, flags=re.DOTALL)
        abstract = re.sub(r'<ref[^>]*\s*/>', '', abstract)
        # Remove HTML comments
        abstract = re.sub(r'<!--.*?-->', '', abstract, flags=re.DOTALL)
        # Remove bold/italic markup
        abstract = re.sub(r"'''?", '', abstract)

        # Limit length
        if len(abstract) > max_length:
            abstract = abstract[:max_length] + "..."

        return abstract.strip()

    return ""


def clean_wikitext_to_plaintext(text: str) -> str:
    """
    Convert wikitext markup to plain text.
    Removes all wiki markup, templates, references, etc.

    This is a best-effort conversion that preserves readable content
    while removing structural markup.
    """
    if not text:
        return ""

    # Skip redirect pages
    if text.strip().startswith("#REDIRECT") or text.strip().startswith("#redirect"):
        return ""

    # Remove HTML comments
    text = re.sub(r'<!--.*?-->', '', text, flags=re.DOTALL)

    # Remove <ref> tags and their content
    text = re.sub(r'<ref[^>]*>.*?</ref>', '', text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<ref[^>]*\s*/>', '', text, flags=re.IGNORECASE)

    # Remove other HTML-like tags but keep content
    text = re.sub(r'</?[a-zA-Z][^>]*>', '', text)

    # Remove templates {{ }} (nested templates handled roughly)
    # This is a simplified approach - doesn't perfectly handle all nesting
    # Limit iterations to prevent performance issues
    max_iterations = 10
    iteration = 0
    while '{{' in text and iteration < max_iterations:
        # Find innermost templates first
        prev_text = text
        text = re.sub(r'\{\{[^{}]*?\}\}', '', text)
        iteration += 1
        # Early exit if no change (avoids infinite loops)
        if prev_text == text:
            break

    # If still templates remaining after iterations, do aggressive removal
    if '{{' in text:
        text = re.sub(r'\{\{[^\}]*\}\}', '', text)

    # Remove file/image links [[File:...]] [[Image:...]]
    text = re.sub(r'\[\[(File|Image):[^\]]+\]\]', '', text, flags=re.IGNORECASE)

    # Remove category links
    text = re.sub(r'\[\[Category:[^\]]+\]\]', '', text, flags=re.IGNORECASE)

    # Convert internal links to plain text
    # [[link|display text]] -> display text
    text = re.sub(r'\[\[([^|\]]+)\|([^\]]+)\]\]', r'\2', text)
    # [[link]] -> link
    text = re.sub(r'\[\[([^\]]+)\]\]', r'\1', text)

    # Remove external links but keep display text
    # [http://example.com display text] -> display text
    text = re.sub(r'\[https?://[^\s\]]+\s+([^\]]+)\]', r'\1', text)
    # [http://example.com] -> (remove)
    text = re.sub(r'\[https?://[^\s\]]+\]', '', text)

    # Remove bold/italic markup
    text = re.sub(r"'{2,}", '', text)

    # Remove section headers but keep text
    text = re.sub(r'^={2,}\s*(.+?)\s*={2,}$', r'\1', text, flags=re.MULTILINE)

    # Remove table markup (basic)
    text = re.sub(r'^\{\|.*?\|\}', '', text, flags=re.MULTILINE | re.DOTALL)
    text = re.sub(r'^\|-.*?$', '', text, flags=re.MULTILINE)
    text = re.sub(r'^\|[\+!].*?$', '', text, flags=re.MULTILINE)

    # Clean up whitespace
    text = re.sub(r'\n{3,}', '\n\n', text)  # Collapse multiple newlines
    text = re.sub(r'[ \t]+', ' ', text)  # Collapse spaces

    # Remove lines that are just punctuation or whitespace
    lines = text.split('\n')
    cleaned_lines = [line.strip() for line in lines if line.strip() and not re.match(r'^[\s\|\-\+\*\#\:;]*$', line.strip())]

    return '\n'.join(cleaned_lines).strip()


def split_pages_from_dump(dump_text: str, max_pages: Optional[int] = None) -> List[str]:
    """
    Split a Wikipedia dump into individual page XML blocks.
    Returns list of <page>...</page> strings.
    """
    pages = []
    page_pattern = re.compile(r'<page>(.*?)</page>', re.DOTALL)

    for match in page_pattern.finditer(dump_text):
        pages.append('<page>' + match.group(1) + '</page>')
        if max_pages and len(pages) >= max_pages:
            break

    return pages