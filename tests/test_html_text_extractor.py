import re
import unittest
from pathlib import Path

from crawler.extractor import HtmlTextExtractor


FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "html"


class HtmlTextExtractorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.extractor = HtmlTextExtractor()

    def test_extract_basic_document_structure(self) -> None:
        html = """
        <!DOCTYPE html>
        <html>
          <head>
            <title>Simple Test Page</title>
            <style>.hidden { display: none; }</style>
            <script>window.should_remove = true;</script>
          </head>
          <body>
            <header>Header Section</header>
            <main>
              <h1>Title</h1>
              <p>Paragraph one with &amp; entity.</p>
              <p>Paragraph two<br>with line break.</p>
              <div>
                <ul>
                  <li>First item</li>
                  <li>Second item</li>
                </ul>
              </div>
            </main>
            <footer>Footer text</footer>
            <!-- comment that should be removed -->
          </body>
        </html>
        """

        result = self.extractor.extract(html)

        expected_lines = [
            "Simple Test Page",
            "Header Section",
            "Title",
            "Paragraph one with & entity.",
            "Paragraph two",
            "with line break.",
            "First item",
            "Second item",
            "Footer text",
        ]

        self.assertEqual("Simple Test Page", result.title)
        self.assertEqual(expected_lines, result.text.split("\n")[: len(expected_lines)])
        self.assertNotIn("window.should_remove", result.text)
        self.assertNotIn("<style>", result.text)
        self.assertNotIn("<script>", result.text)
        self.assertNotIn("<!--", result.text)

    def test_extract_real_html_samples(self) -> None:
        expectations = {
            "repo_v2ray_core.html": [
                "GitHub - v2ray/v2ray-core: A platform for building proxies to bypass network restrictions.",
                "Latest commit",
                "MIT License",
            ],
            "issue_fastapi_snippet.html": [
                "Can't use `Annotated` with `ForwardRef` · Issue #13056 · fastapi/fastapi · GitHub",
                "Can't use Annotated with ForwardRef #13056",
                "from typing import Annotated",
            ],
        }

        for fixture_name, snippets in expectations.items():
            with self.subTest(fixture=fixture_name):
                html_path = FIXTURE_ROOT / fixture_name
                html = html_path.read_text("utf-8", errors="ignore")
                result = self.extractor.extract(html)

                self.assertTrue(result.title)
                self.assertTrue(result.text)
                for snippet in snippets:
                    self.assertIn(snippet, result.text)

                self.assertNotIn("\n\n\n", result.text)
                self.assertIsNone(re.search(r"<[^>]+>", result.text))

    def test_extract_text_alias(self) -> None:
        html = "<html><head><title>Alias</title></head><body><p>Body</p></body></html>"
        text = self.extractor.extract_text(html)
        self.assertEqual("Alias\nBody", text)


if __name__ == "__main__":
    unittest.main()
