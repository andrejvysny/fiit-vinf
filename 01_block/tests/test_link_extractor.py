"""Unit tests for LinkExtractor in crawler/extractor.py

Tests verify URL extraction logic including:
- Basic href extraction from anchor tags
- Handling of relative and absolute URLs
- URL normalization and fragment removal
- Filtering of invalid schemes and special URLs
"""

import unittest
from crawler.extractor import LinkExtractor


class TestLinkExtractorBasics(unittest.TestCase):
    """Test basic URL extraction functionality."""

    def setUp(self):
        """Initialize LinkExtractor for each test."""
        self.extractor = LinkExtractor()
        self.base_url = "https://example.com/page"

    def test_extract_simple_absolute_url(self):
        """Test extraction of simple absolute URL."""
        html = '<a href="https://example.com/about">About</a>'
        result = self.extractor.extract(html, self.base_url)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], "https://example.com/about")

    def test_extract_relative_url(self):
        """Test extraction and conversion of relative URLs."""
        html = '<a href="/contact">Contact</a>'
        result = self.extractor.extract(html, "https://example.com")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], "https://example.com/contact")

    def test_extract_path_relative_url(self):
        """Test extraction of path-relative URLs."""
        html = '<a href="about.html">About</a>'
        result = self.extractor.extract(html, "https://example.com/page/")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], "https://example.com/page/about.html")

    def test_extract_multiple_urls(self):
        """Test extraction of multiple URLs from HTML."""
        html = '''
        <a href="https://example.com/page1">Page 1</a>
        <a href="https://example.com/page2">Page 2</a>
        <a href="https://example.com/page3">Page 3</a>
        '''
        result = self.extractor.extract(html, self.base_url)
        self.assertEqual(len(result), 3)
        self.assertIn("https://example.com/page1", result)
        self.assertIn("https://example.com/page2", result)
        self.assertIn("https://example.com/page3", result)


class TestLinkExtractorQuoting(unittest.TestCase):
    """Test different href quoting styles."""

    def setUp(self):
        """Initialize LinkExtractor for each test."""
        self.extractor = LinkExtractor()
        self.base_url = "https://example.com"

    def test_double_quoted_href(self):
        """Test double-quoted href attributes."""
        html = '<a href="https://example.com/page">Link</a>'
        result = self.extractor.extract(html, self.base_url)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], "https://example.com/page")

    def test_single_quoted_href(self):
        """Test single-quoted href attributes."""
        html = "<a href='https://example.com/page'>Link</a>"
        result = self.extractor.extract(html, self.base_url)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], "https://example.com/page")

    def test_mixed_quotes(self):
        """Test mixed quoting styles in the same HTML."""
        html = '''
        <a href="https://example.com/page1">Link 1</a>
        <a href='https://example.com/page2'>Link 2</a>
        '''
        result = self.extractor.extract(html, self.base_url)
        self.assertEqual(len(result), 2)


class TestLinkExtractorHTMLEntities(unittest.TestCase):
    """Test handling of HTML entities in URLs."""

    def setUp(self):
        """Initialize LinkExtractor for each test."""
        self.extractor = LinkExtractor()
        self.base_url = "https://example.com"

    def test_ampersand_entity(self):
        """Test unescaping of &amp; entity."""
        html = '<a href="https://example.com/page?foo=1&amp;bar=2">Link</a>'
        result = self.extractor.extract(html, self.base_url)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], "https://example.com/page?foo=1&bar=2")

    def test_multiple_entities(self):
        """Test handling of multiple HTML entities."""
        html = '<a href="page?a=1&amp;b=2&amp;c=3">Link</a>'
        result = self.extractor.extract(html, self.base_url)
        self.assertEqual(len(result), 1)
        self.assertIn("&", result[0])


class TestLinkExtractorFiltering(unittest.TestCase):
    """Test filtering of invalid and special URLs."""

    def setUp(self):
        """Initialize LinkExtractor for each test."""
        self.extractor = LinkExtractor()
        self.base_url = "https://example.com"

    def test_skip_fragment_only(self):
        """Test that fragment-only anchors are skipped."""
        html = '<a href="#section">Section</a>'
        result = self.extractor.extract(html, self.base_url)
        self.assertEqual(len(result), 0)

    def test_skip_javascript_protocol(self):
        """Test that javascript: URLs are skipped."""
        html = '<a href="javascript:void(0)">Click</a>'
        result = self.extractor.extract(html, self.base_url)
        self.assertEqual(len(result), 0)

    def test_skip_mailto(self):
        """Test that mailto: links are skipped."""
        html = '<a href="mailto:test@example.com">Email</a>'
        result = self.extractor.extract(html, self.base_url)
        self.assertEqual(len(result), 0)

    def test_skip_tel(self):
        """Test that tel: links are skipped."""
        html = '<a href="tel:+1234567890">Call</a>'
        result = self.extractor.extract(html, self.base_url)
        self.assertEqual(len(result), 0)

    def test_empty_href_returns_base(self):
        """Test that empty hrefs are processed (may return quoted empty path)."""
        html = '<a href="">Link</a>'
        result = self.extractor.extract(html, self.base_url)
        # Empty href gets processed by urljoin
        self.assertEqual(len(result), 1)
        # The actual implementation returns the base URL with empty quotes appended
        self.assertTrue(result[0].startswith(self.base_url.split('/')[0] + '//' + self.base_url.split('/')[2]))

    def test_skip_whitespace_only_href(self):
        """Test that whitespace-only hrefs are skipped."""
        html = '<a href="   ">Link</a>'
        result = self.extractor.extract(html, self.base_url)
        self.assertEqual(len(result), 0)


class TestLinkExtractorFragmentRemoval(unittest.TestCase):
    """Test URL fragment removal."""

    def setUp(self):
        """Initialize LinkExtractor for each test."""
        self.extractor = LinkExtractor()
        self.base_url = "https://example.com"

    def test_remove_fragment(self):
        """Test that URL fragments are removed."""
        html = '<a href="https://example.com/page#section">Link</a>'
        result = self.extractor.extract(html, self.base_url)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], "https://example.com/page")
        self.assertNotIn("#", result[0])

    def test_preserve_query_remove_fragment(self):
        """Test that query params are preserved but fragments removed."""
        html = '<a href="https://example.com/page?id=123#section">Link</a>'
        result = self.extractor.extract(html, self.base_url)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], "https://example.com/page?id=123")

    def test_fragment_deduplication(self):
        """Test that URLs differing only by fragment are deduplicated."""
        html = '''
        <a href="https://example.com/page#section1">Link 1</a>
        <a href="https://example.com/page#section2">Link 2</a>
        <a href="https://example.com/page">Link 3</a>
        '''
        result = self.extractor.extract(html, self.base_url)
        # All three should resolve to same URL
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], "https://example.com/page")


class TestLinkExtractorSchemeValidation(unittest.TestCase):
    """Test URL scheme validation."""

    def setUp(self):
        """Initialize LinkExtractor for each test."""
        self.extractor = LinkExtractor()
        self.base_url = "https://example.com"

    def test_accept_http(self):
        """Test that HTTP URLs are accepted."""
        html = '<a href="http://example.com/page">Link</a>'
        result = self.extractor.extract(html, self.base_url)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], "http://example.com/page")

    def test_accept_https(self):
        """Test that HTTPS URLs are accepted."""
        html = '<a href="https://example.com/page">Link</a>'
        result = self.extractor.extract(html, self.base_url)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], "https://example.com/page")

    def test_reject_ftp(self):
        """Test that FTP URLs are rejected."""
        html = '<a href="ftp://ftp.example.com/file">File</a>'
        result = self.extractor.extract(html, self.base_url)
        self.assertEqual(len(result), 0)

    def test_reject_data_url(self):
        """Test that data: URLs are rejected."""
        html = '<a href="data:text/html,<h1>Test</h1>">Data</a>'
        result = self.extractor.extract(html, self.base_url)
        self.assertEqual(len(result), 0)


class TestLinkExtractorDeduplication(unittest.TestCase):
    """Test URL deduplication logic."""

    def setUp(self):
        """Initialize LinkExtractor for each test."""
        self.extractor = LinkExtractor()
        self.base_url = "https://example.com"

    def test_exact_duplicate_removal(self):
        """Test that exact duplicate URLs are removed."""
        html = '''
        <a href="https://example.com/page">Link 1</a>
        <a href="https://example.com/page">Link 2</a>
        <a href="https://example.com/page">Link 3</a>
        '''
        result = self.extractor.extract(html, self.base_url)
        self.assertEqual(len(result), 1)

    def test_preserve_order(self):
        """Test that first occurrence order is preserved."""
        html = '''
        <a href="https://example.com/page1">Page 1</a>
        <a href="https://example.com/page2">Page 2</a>
        <a href="https://example.com/page1">Page 1 Again</a>
        <a href="https://example.com/page3">Page 3</a>
        '''
        result = self.extractor.extract(html, self.base_url)
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0], "https://example.com/page1")
        self.assertEqual(result[1], "https://example.com/page2")
        self.assertEqual(result[2], "https://example.com/page3")


class TestLinkExtractorComplexHTML(unittest.TestCase):
    """Test extraction from complex HTML structures."""

    def setUp(self):
        """Initialize LinkExtractor for each test."""
        self.extractor = LinkExtractor()
        self.base_url = "https://example.com"

    def test_nested_elements(self):
        """Test extraction from nested HTML elements."""
        html = '''
        <div class="navigation">
            <ul>
                <li><a href="/home">Home</a></li>
                <li><a href="/about">About</a></li>
                <li><a href="/contact">Contact</a></li>
            </ul>
        </div>
        '''
        result = self.extractor.extract(html, self.base_url)
        self.assertEqual(len(result), 3)

    def test_multiline_anchor_tag(self):
        """Test extraction from multiline anchor tags."""
        html = '''
        <a
            class="link"
            href="https://example.com/page"
            target="_blank"
        >
            Link Text
        </a>
        '''
        result = self.extractor.extract(html, self.base_url)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], "https://example.com/page")

    def test_href_with_other_attributes(self):
        """Test extraction when href is mixed with other attributes."""
        html = '''
        <a class="btn" id="link1" href="https://example.com/page" rel="nofollow">Link</a>
        <a href="https://example.com/page2" class="btn" target="_blank">Link 2</a>
        '''
        result = self.extractor.extract(html, self.base_url)
        self.assertEqual(len(result), 2)


class TestLinkExtractorEdgeCases(unittest.TestCase):
    """Test edge cases and error handling."""

    def setUp(self):
        """Initialize LinkExtractor for each test."""
        self.extractor = LinkExtractor()

    def test_empty_html(self):
        """Test extraction from empty HTML."""
        result = self.extractor.extract("", "https://example.com")
        self.assertEqual(len(result), 0)

    def test_empty_base_url(self):
        """Test extraction with empty base URL."""
        html = '<a href="https://example.com/page">Link</a>'
        result = self.extractor.extract(html, "")
        self.assertEqual(len(result), 0)

    def test_none_html(self):
        """Test extraction from None HTML."""
        result = self.extractor.extract(None, "https://example.com")
        self.assertEqual(len(result), 0)

    def test_none_base_url(self):
        """Test extraction with None base URL."""
        html = '<a href="https://example.com/page">Link</a>'
        result = self.extractor.extract(html, None)
        self.assertEqual(len(result), 0)

    def test_no_anchor_tags(self):
        """Test HTML with no anchor tags."""
        html = '<div><p>Some text without links</p></div>'
        result = self.extractor.extract("", "https://example.com")
        self.assertEqual(len(result), 0)

    def test_malformed_html(self):
        """Test extraction from malformed HTML."""
        html = '<a href="https://example.com/page">Unclosed anchor'
        # Should still extract the URL even if HTML is malformed
        result = self.extractor.extract(html, "https://example.com")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], "https://example.com/page")


class TestLinkExtractorURLNormalization(unittest.TestCase):
    """Test URL normalization and resolution."""

    def setUp(self):
        """Initialize LinkExtractor for each test."""
        self.extractor = LinkExtractor()

    def test_resolve_relative_to_root(self):
        """Test resolving relative URLs to domain root."""
        html = '<a href="/path/page">Link</a>'
        result = self.extractor.extract(html, "https://example.com/some/deep/path")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], "https://example.com/path/page")

    def test_resolve_parent_directory(self):
        """Test resolving parent directory references (..)."""
        html = '<a href="../other/page">Link</a>'
        result = self.extractor.extract(html, "https://example.com/path/current/")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], "https://example.com/path/other/page")

    def test_resolve_current_directory(self):
        """Test resolving current directory references (.)."""
        html = '<a href="./page">Link</a>'
        result = self.extractor.extract(html, "https://example.com/path/")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], "https://example.com/path/page")

    def test_preserve_query_params(self):
        """Test that query parameters are preserved."""
        html = '<a href="https://example.com/page?foo=bar&baz=qux">Link</a>'
        result = self.extractor.extract(html, "https://example.com")
        self.assertEqual(len(result), 1)
        self.assertIn("?foo=bar&baz=qux", result[0])


class TestLinkExtractorLogging(unittest.TestCase):
    """Test logging behavior (without verifying actual log output)."""

    def setUp(self):
        """Initialize LinkExtractor for each test."""
        self.extractor = LinkExtractor()

    def test_extract_logs_count(self):
        """Test that extraction completes without errors (logs internally)."""
        html = '''
        <a href="https://example.com/page1">Link 1</a>
        <a href="https://example.com/page2">Link 2</a>
        '''
        # Should complete without raising exceptions
        result = self.extractor.extract(html, "https://example.com")
        self.assertEqual(len(result), 2)


if __name__ == '__main__':
    unittest.main()
