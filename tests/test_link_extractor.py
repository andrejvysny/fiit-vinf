import unittest

from crawler.extractor import LinkExtractor


class LinkExtractorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.extractor = LinkExtractor()

    def test_extract_filters_and_normalizes_links(self) -> None:
        html = """
        <a href="page1.html">Relative</a>
        <a href='/abs/path'>Absolute Path</a>
        <a href="//github.com/other/repo">Schemaless</a>
        <a href="page1.html#section">Fragment</a>
        <a href="javascript:void(0)">JS</a>
        <a href="#skip">Skip</a>
        <a href="mailto:dev@example.com">Email</a>
        <a href="tel:123">Phone</a>
        <a href=" HTTP://EXAMPLE.COM/Mixed ">Mixed Case</a>
        <a href=page2.html>Unquoted</a>
        """
        base_url = "https://github.com/org/repo/"

        links = self.extractor.extract(html, base_url)

        expected = [
            "https://github.com/org/repo/page1.html",
            "https://github.com/abs/path",
            "https://github.com/other/repo",
            "http://EXAMPLE.COM/Mixed",
            "https://github.com/org/repo/page2.html",
        ]

        self.assertEqual(expected, links)

    def test_extract_handles_duplicate_urls(self) -> None:
        html = """
        <a href="https://example.com/a">First</a>
        <a href="https://example.com/a#fragment">Duplicate with fragment</a>
        <a href="https://example.com/a">Duplicate exact</a>
        """

        links = self.extractor.extract(html, "https://example.com/index")

        self.assertEqual(["https://example.com/a"], links)


if __name__ == "__main__":
    unittest.main()
