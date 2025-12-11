"""Unit tests for regex patterns in extractor/regexes.py

Tests verify that all compiled regex patterns correctly match and extract
expected data from HTML content and code samples.
"""

import re
import unittest
from extractor import regexes


class TestHTMLCleaningRegexes(unittest.TestCase):
    """Test HTML cleaning and parsing regex patterns."""

    def test_html_comment_removal(self):
        """Test HTML comment regex removes comments correctly."""
        pattern = regexes.get_html_comment_regex()

        html = "Hello <!-- this is a comment --> World"
        result = pattern.sub("", html)
        self.assertEqual(result, "Hello  World")

        # Multi-line comment
        html_multi = "Start <!-- multi\nline\ncomment --> End"
        result_multi = pattern.sub("", html_multi)
        self.assertEqual(result_multi, "Start  End")

    def test_html_doctype_removal(self):
        """Test DOCTYPE declaration regex removal."""
        pattern = regexes.get_html_doctype_regex()

        html = "<!DOCTYPE html><html><body>Content</body></html>"
        result = pattern.sub("", html)
        self.assertEqual(result, "<html><body>Content</body></html>")

        # Case insensitive
        html_upper = "<!doctype HTML><p>Text</p>"
        result_upper = pattern.sub("", html_upper)
        self.assertEqual(result_upper, "<p>Text</p>")

    def test_html_self_closing_tags(self):
        """Test self-closing tags removal (meta, link, img, etc.)."""
        pattern = regexes.get_html_self_closing_regex()

        # Image tag
        html = '<p>Text <img src="image.png"/> more text</p>'
        result = pattern.sub("", html)
        self.assertEqual(result, '<p>Text  more text</p>')

        # Meta tag
        html_meta = '<meta charset="utf-8"><p>Content</p>'
        result_meta = pattern.sub("", html_meta)
        self.assertEqual(result_meta, '<p>Content</p>')

        # Multiple self-closing tags
        html_multi = '<link rel="stylesheet" href="style.css"><br><hr/><p>Text</p>'
        result_multi = pattern.sub("", html_multi)
        self.assertEqual(result_multi, '<p>Text</p>')

    def test_html_remove_script_style_tags(self):
        """Test removal of script, style and other unwanted tags with content."""
        pattern = regexes.get_html_remove_tags_regex()

        # Script tag
        html = '<p>Text</p><script>alert("hi")</script><p>More</p>'
        result = pattern.sub("", html)
        self.assertEqual(result, '<p>Text</p><p>More</p>')

        # Style tag
        html_style = '<p>Text</p><style>body{color:red}</style><p>More</p>'
        result_style = pattern.sub("", html_style)
        self.assertEqual(result_style, '<p>Text</p><p>More</p>')

        # Case insensitive
        html_upper = '<p>Text</p><SCRIPT>code</SCRIPT><p>More</p>'
        result_upper = pattern.sub("", html_upper)
        self.assertEqual(result_upper, '<p>Text</p><p>More</p>')

    def test_html_br_hr_replacement(self):
        """Test <br> and <hr> tag matching."""
        pattern = regexes.get_html_br_hr_regex()

        self.assertTrue(pattern.search("<br>"))
        self.assertTrue(pattern.search("<br/>"))
        self.assertTrue(pattern.search("<br />"))
        self.assertTrue(pattern.search("<BR>"))
        self.assertTrue(pattern.search("<hr>"))
        self.assertTrue(pattern.search("<hr/>"))

    def test_html_block_tags(self):
        """Test block-level HTML tag matching."""
        pattern = regexes.get_html_block_tags_regex()

        # Test common block tags
        self.assertTrue(pattern.search("<p>"))
        self.assertTrue(pattern.search("</p>"))
        self.assertTrue(pattern.search("<div class='test'>"))
        self.assertTrue(pattern.search("<h1>"))
        self.assertTrue(pattern.search("<section>"))
        self.assertTrue(pattern.search("<article>"))
        self.assertTrue(pattern.search("</div>"))

    def test_html_generic_tag_removal(self):
        """Test generic HTML tag removal."""
        pattern = regexes.get_html_generic_tag_regex()

        html = "<strong>bold</strong> and <em>italic</em>"
        result = pattern.sub("", html)
        self.assertEqual(result, "bold and italic")

    def test_html_multi_space_collapse(self):
        """Test multiple space collapsing."""
        pattern = regexes.get_html_multi_space_regex()

        text = "Hello    world   with     spaces"
        result = pattern.sub(" ", text)
        self.assertEqual(result, "Hello world with spaces")

    def test_html_multi_newline_collapse(self):
        """Test excessive newline collapsing."""
        pattern = regexes.get_html_multi_newline_regex()

        text = "Line1\n\n\n\nLine2\n\n\n\n\nLine3"
        result = pattern.sub("\n\n", text)
        self.assertEqual(result, "Line1\n\nLine2\n\nLine3")


class TestGitHubMetadataRegexes(unittest.TestCase):
    """Test GitHub-specific metadata extraction patterns."""

    def test_star_counter_extraction(self):
        """Test star count extraction from span element."""
        patterns = regexes.get_star_regexes()
        star_counter_re = patterns[0]

        html = '<span id="repo-stars-counter-star" title="34,222">34.2k</span>'
        match = star_counter_re.search(html)
        self.assertIsNotNone(match)
        self.assertEqual(match.group(1), "34,222")

        # Test another format
        html2 = '<span id="repo-stars-counter-star" data-view-component="true" title="1,234">1.2k</span>'
        match2 = star_counter_re.search(html2)
        self.assertIsNotNone(match2)
        self.assertEqual(match2.group(1), "1,234")

    def test_star_aria_label(self):
        """Test star count extraction from aria-label."""
        patterns = regexes.get_star_regexes()
        star_aria_re = patterns[1]

        html = 'aria-label="1,234 users starred this repository"'
        match = star_aria_re.search(html)
        self.assertIsNotNone(match)
        self.assertEqual(match.group(1), "1,234")

        # Singular form
        html_single = 'aria-label="1 user starred this repository"'
        match_single = star_aria_re.search(html_single)
        self.assertIsNotNone(match_single)
        self.assertEqual(match_single.group(1), "1")

    def test_fork_counter_extraction(self):
        """Test fork count extraction."""
        patterns = regexes.get_fork_regexes()
        fork_re = patterns[0]

        html = '<span id="repo-network-counter" title="12,790">12.8k</span>'
        match = fork_re.search(html)
        self.assertIsNotNone(match)
        self.assertEqual(match.group(1), "12,790")

    def test_language_name_extraction(self):
        """Test programming language name extraction."""
        lang_patterns = regexes.get_lang_stats_regexes()
        lang_name_re = lang_patterns['name']

        html = '<span itemprop="programmingLanguage">Python</span>'
        match = lang_name_re.search(html)
        self.assertIsNotNone(match)
        self.assertEqual(match.group(1), "Python")

        # Multiple languages
        html_multi = '''
        <span itemprop="programmingLanguage">JavaScript</span>
        <span itemprop="programmingLanguage">TypeScript</span>
        '''
        matches = lang_name_re.findall(html_multi)
        self.assertEqual(len(matches), 2)
        self.assertIn("JavaScript", matches)
        self.assertIn("TypeScript", matches)

    def test_language_percentage_extraction(self):
        """Test language percentage extraction."""
        lang_patterns = regexes.get_lang_stats_regexes()
        percent_re = lang_patterns['percent']

        text = "Python 73.5% JavaScript 20.1% CSS 6.4%"
        matches = percent_re.findall(text)
        self.assertEqual(len(matches), 3)
        self.assertIn("73.5", matches)
        self.assertIn("20.1", matches)
        self.assertIn("6.4", matches)


class TestLicenseRegexes(unittest.TestCase):
    """Test license detection patterns."""

    def test_spdx_license_detection(self):
        """Test SPDX license identifier matching."""
        license_patterns = regexes.get_license_regexes()
        spdx_re = license_patterns[0]

        # Test common licenses
        self.assertIsNotNone(spdx_re.search("This project is MIT licensed"))
        self.assertIsNotNone(spdx_re.search("Licensed under Apache-2.0"))
        self.assertIsNotNone(spdx_re.search("GPL-3.0 license"))
        self.assertIsNotNone(spdx_re.search("BSD-3-Clause"))

        # Case insensitive
        self.assertIsNotNone(spdx_re.search("mit license"))

        # Word boundaries prevent partial matches
        self.assertIsNone(spdx_re.search("MITIGATION"))

    def test_license_link_extraction(self):
        """Test license link extraction from HTML."""
        license_patterns = regexes.get_license_regexes()
        link_re = license_patterns[1]

        html = '<a href="/user/repo/blob/main/LICENSE">MIT License</a>'
        match = link_re.search(html)
        self.assertIsNotNone(match)
        self.assertEqual(match.group(1), "MIT License")


class TestTopicRegex(unittest.TestCase):
    """Test GitHub topic/tag extraction."""

    def test_topic_tag_extraction(self):
        """Test topic tag extraction from GitHub HTML."""
        topic_re = regexes.get_topic_regex()

        html = '<a data-octo-click="topic" class="topic-tag">machine-learning</a>'
        match = topic_re.search(html)
        self.assertIsNotNone(match)
        self.assertEqual(match.group(1), "machine-learning")

        # Multiple topics
        html_multi = '''
        <a class="topic-tag">python</a>
        <a class="topic-tag">data-science</a>
        <a class="topic-tag">ai</a>
        '''
        matches = topic_re.findall(html_multi)
        self.assertEqual(len(matches), 3)
        self.assertIn("python", matches)
        self.assertIn("data-science", matches)


class TestImportRegexes(unittest.TestCase):
    """Test import statement extraction patterns."""

    def test_python_imports(self):
        """Test Python import statement matching."""
        import_patterns = regexes.get_import_regexes()
        python_re = import_patterns['python']

        # Simple import
        code = "import os"
        match = python_re.search(code)
        self.assertIsNotNone(match)

        # From import
        code_from = "from typing import List, Dict"
        match_from = python_re.search(code_from)
        self.assertIsNotNone(match_from)

        # Multiple imports
        code_multi = """
import sys
import os
from pathlib import Path
from typing import Optional
"""
        matches = python_re.findall(code_multi)
        self.assertGreaterEqual(len(matches), 4)

    def test_javascript_imports(self):
        """Test JavaScript/TypeScript import matching."""
        import_patterns = regexes.get_import_regexes()
        js_re = import_patterns['javascript']

        # ES6 import
        code = 'import React from "react"'
        matches = js_re.findall(code)
        self.assertTrue(any("react" in str(m) for m in matches))

        # Require statement
        code_require = 'const fs = require("fs")'
        matches_req = js_re.findall(code_require)
        self.assertTrue(any("fs" in str(m) for m in matches_req))

    def test_rust_imports(self):
        """Test Rust use statement matching."""
        import_patterns = regexes.get_import_regexes()
        rust_re = import_patterns['rust']

        code = "use std::collections::HashMap;"
        match = rust_re.search(code)
        self.assertIsNotNone(match)

    def test_c_includes(self):
        """Test C/C++ include matching."""
        import_patterns = regexes.get_import_regexes()
        c_re = import_patterns['c']

        # System header
        code = '#include <stdio.h>'
        match = c_re.search(code)
        self.assertIsNotNone(match)
        self.assertEqual(match.group(1), "stdio.h")

        # Local header
        code_local = '#include "myheader.h"'
        match_local = c_re.search(code_local)
        self.assertIsNotNone(match_local)
        self.assertEqual(match_local.group(1), "myheader.h")


class TestURLRegexes(unittest.TestCase):
    """Test URL extraction patterns."""

    def test_http_url_extraction(self):
        """Test HTTP/HTTPS URL extraction from text."""
        url_patterns = regexes.get_url_regexes()
        http_re = url_patterns['http']

        text = "Visit https://github.com/user/repo for more info"
        match = http_re.search(text)
        self.assertIsNotNone(match)
        self.assertEqual(match.group(0), "https://github.com/user/repo")

        # HTTP
        text_http = "Go to http://example.com"
        match_http = http_re.search(text_http)
        self.assertIsNotNone(match_http)
        self.assertEqual(match_http.group(0), "http://example.com")

    def test_markdown_link_extraction(self):
        """Test markdown link extraction."""
        url_patterns = regexes.get_url_regexes()
        markdown_re = url_patterns['markdown']

        md = "[GitHub](https://github.com)"
        match = markdown_re.search(md)
        self.assertIsNotNone(match)
        self.assertEqual(match.group(1), "GitHub")
        self.assertEqual(match.group(2), "https://github.com")

    def test_html_href_extraction(self):
        """Test HTML href attribute extraction."""
        url_patterns = regexes.get_url_regexes()
        href_re = url_patterns['html']

        # Double quotes
        html = '<a href="https://example.com">Link</a>'
        match = href_re.search(html)
        self.assertIsNotNone(match)
        self.assertEqual(match.group(1), "https://example.com")

        # Single quotes
        html_single = "<a href='https://example.com'>Link</a>"
        match_single = href_re.search(html_single)
        self.assertIsNotNone(match_single)
        self.assertEqual(match_single.group(1), "https://example.com")


class TestVersionRegex(unittest.TestCase):
    """Test semantic version extraction."""

    def test_semver_extraction(self):
        """Test semantic version pattern matching."""
        version_re = regexes.get_version_regex()

        # Simple version
        text = "Version 1.2.3"
        match = version_re.search(text)
        self.assertIsNotNone(match)
        self.assertEqual(match.group(1), "1.2.3")

        # With v prefix
        text_v = "v2.0.0"
        match_v = version_re.search(text_v)
        self.assertIsNotNone(match_v)
        self.assertEqual(match_v.group(1), "2.0.0")

        # Pre-release
        text_pre = "v1.0.0-alpha.1"
        match_pre = version_re.search(text_pre)
        self.assertIsNotNone(match_pre)
        self.assertEqual(match_pre.group(1), "1.0.0-alpha.1")

        # Build metadata
        text_build = "1.0.0+build.123"
        match_build = version_re.search(text_build)
        self.assertIsNotNone(match_build)
        self.assertEqual(match_build.group(1), "1.0.0+build.123")


class TestEmailRegex(unittest.TestCase):
    """Test email extraction pattern."""

    def test_email_extraction(self):
        """Test email address extraction."""
        email_re = regexes.get_email_regex()

        text = "Contact us at support@example.com for help"
        match = email_re.search(text)
        self.assertIsNotNone(match)
        self.assertEqual(match.group(1), "support@example.com")

        # With dots and numbers
        text_complex = "Email: john.doe123@company.co.uk"
        match_complex = email_re.search(text_complex)
        self.assertIsNotNone(match_complex)
        self.assertEqual(match_complex.group(1), "john.doe123@company.co.uk")


class TestCodeFenceRegex(unittest.TestCase):
    """Test code fence language detection."""

    def test_code_fence_language(self):
        """Test code fence language hint extraction."""
        code_re = regexes.get_code_lang_regex()

        # Python
        markdown = "```python\nprint('hello')\n```"
        match = code_re.search(markdown)
        self.assertIsNotNone(match)
        self.assertEqual(match.group(1), "python")

        # JavaScript
        markdown_js = "```javascript\nconsole.log('hi');\n```"
        match_js = code_re.search(markdown_js)
        self.assertIsNotNone(match_js)
        self.assertEqual(match_js.group(1), "javascript")

        # C++
        markdown_cpp = "```c++\nint main() {}\n```"
        match_cpp = code_re.search(markdown_cpp)
        self.assertIsNotNone(match_cpp)
        self.assertEqual(match_cpp.group(1), "c++")


class TestIssueReferenceRegexes(unittest.TestCase):
    """Test GitHub issue reference extraction."""

    def test_simple_issue_reference(self):
        """Test simple issue reference (#123)."""
        issue_patterns = regexes.get_issue_ref_regexes()
        issue_ref_re = issue_patterns[0]

        text = "Fixed in #123"
        match = issue_ref_re.search(text)
        self.assertIsNotNone(match)
        self.assertEqual(match.group(1), "123")

    def test_cross_repo_issue_reference(self):
        """Test cross-repository issue reference (owner/repo#123)."""
        issue_patterns = regexes.get_issue_ref_regexes()
        issue_ref_re = issue_patterns[0]

        text = "See user/repo#456"
        match = issue_ref_re.search(text)
        self.assertIsNotNone(match)
        self.assertEqual(match.group(4), "456")

    def test_issue_url(self):
        """Test issue URL extraction."""
        issue_patterns = regexes.get_issue_ref_regexes()
        issue_url_re = issue_patterns[1]

        url = "https://github.com/owner/repo/issues/789"
        match = issue_url_re.search(url)
        self.assertIsNotNone(match)
        self.assertEqual(match.group(1), "owner")
        self.assertEqual(match.group(2), "repo")
        self.assertEqual(match.group(3), "789")


if __name__ == '__main__':
    unittest.main()
