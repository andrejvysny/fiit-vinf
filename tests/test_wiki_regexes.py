#!/usr/bin/env python3
"""Unit tests for Wikipedia regex patterns."""

import unittest
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from spark.lib.wiki_regexes import (
    extract_page_xml,
    normalize_title,
    extract_categories,
    extract_internal_links,
    extract_infobox_fields,
    extract_abstract,
)


class TestWikiRegexes(unittest.TestCase):
    """Test Wikipedia regex extraction patterns."""

    def test_extract_page_xml(self):
        """Test extraction of page fields from XML."""
        page_xml = """<page>
    <title>Python (programming language)</title>
    <ns>0</ns>
    <id>23862</id>
    <revision>
      <timestamp>2024-09-01T10:00:00Z</timestamp>
      <text>Python is a high-level programming language.</text>
    </revision>
  </page>"""

        result = extract_page_xml(page_xml)
        self.assertIsNotNone(result)
        self.assertEqual(result['page_id'], 23862)
        self.assertEqual(result['title'], 'Python (programming language)')
        self.assertEqual(result['namespace'], 0)
        self.assertIsNone(result['redirect_to'])
        self.assertIn('high-level programming', result['text'])

    def test_extract_redirect(self):
        """Test extraction of redirect pages."""
        page_xml = """<page>
    <title>MIT License</title>
    <ns>0</ns>
    <id>12345</id>
    <redirect title="MIT License (software)" />
    <revision>
      <text>#REDIRECT [[MIT License (software)]]</text>
    </revision>
  </page>"""

        result = extract_page_xml(page_xml)
        self.assertIsNotNone(result)
        self.assertEqual(result['redirect_to'], 'MIT License (software)')

    def test_normalize_title(self):
        """Test title normalization."""
        # Basic normalization
        self.assertEqual(normalize_title("Python"), "python")

        # Remove parenthetical suffix
        self.assertEqual(
            normalize_title("Python (programming language)"),
            "python"
        )

        # Handle punctuation
        self.assertEqual(
            normalize_title("C++"),
            "c"
        )

        # Collapse spaces
        self.assertEqual(
            normalize_title("Apache  License   2.0"),
            "apache license 2 0"
        )

        # ASCII folding
        self.assertEqual(
            normalize_title("Réact"),
            "react"
        )

    def test_extract_categories(self):
        """Test category extraction from wikitext."""
        text = """
Python is a programming language.
[[Category:Programming languages]]
[[Category:Cross-platform software|Python]]
[[Category:Object-oriented programming languages]]
        """

        categories = extract_categories(text)
        self.assertEqual(len(categories), 3)
        self.assertIn("Programming languages", categories)
        self.assertIn("Cross-platform software", categories)
        self.assertIn("Object-oriented programming languages", categories)

    def test_extract_internal_links(self):
        """Test internal link extraction."""
        text = """
Python was created by [[Guido van Rossum]].
It is influenced by [[ABC (programming language)|ABC]] and [[Modula-3]].
See also [[File:Python-logo.png]] and [[Category:Programming]].
        """

        links = extract_internal_links(text)
        self.assertEqual(len(links), 3)
        self.assertIn("Guido van Rossum", links)
        self.assertIn("ABC (programming language)", links)
        self.assertIn("Modula-3", links)
        # File and Category links should be excluded
        self.assertNotIn("File:Python-logo.png", links)

    def test_extract_infobox(self):
        """Test infobox field extraction."""
        text = """
{{Infobox programming language
| name = Python
| paradigm = {{unbulleted list|[[Object-oriented programming|Object-oriented]]|[[Imperative programming|Imperative]]}}
| designer = [[Guido van Rossum]]
| developer = Python Software Foundation
| first_appeared = 1991
| typing = [[Duck typing|Duck]], [[dynamic typing|dynamic]], [[strong typing|strong]]
| license = Python Software Foundation License
| website = {{URL|https://www.python.org/}}
}}

Python is a programming language...
        """

        fields = extract_infobox_fields(text)
        self.assertGreater(len(fields), 0)
        self.assertIn("name", fields)
        self.assertEqual(fields["name"], "Python")
        self.assertIn("designer", fields)
        self.assertIn("license", fields)

    def test_extract_abstract(self):
        """Test abstract extraction."""
        text = """Python is a high-level, general-purpose [[programming language]]. Its design philosophy emphasizes code readability with the use of [[significant indentation]].

== History ==
Python was conceived in the late 1980s...
        """

        abstract = extract_abstract(text)
        self.assertIn("Python is a high-level", abstract)
        self.assertIn("code readability", abstract)
        self.assertNotIn("History", abstract)  # Should stop before section
        self.assertNotIn("[[", abstract)  # Wiki markup should be cleaned

    def test_extract_abstract_with_redirect(self):
        """Test that redirects return empty abstract."""
        text = "#REDIRECT [[MIT License]]"
        abstract = extract_abstract(text)
        self.assertEqual(abstract, "")


class TestWikiJoinNormalization(unittest.TestCase):
    """Test normalization for entity-wiki joining."""

    def test_entity_normalization_matches_wiki(self):
        """Ensure entity values normalize the same as wiki titles."""
        # Programming languages
        self.assertEqual(
            normalize_title("Python"),
            normalize_title("Python (programming language)")
        )

        # Licenses
        self.assertEqual(
            normalize_title("MIT"),
            normalize_title("MIT License")
        )

        # Topics with special characters
        self.assertEqual(
            normalize_title("c++"),
            normalize_title("C++")
        )


if __name__ == '__main__':
    unittest.main()