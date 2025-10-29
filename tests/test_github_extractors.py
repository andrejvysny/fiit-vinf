import unittest
from pathlib import Path

from crawler.extractor import HtmlTextExtractor

import extract


FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "html"


class GithubMainContentExtractorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.extractor = extract.GithubMainContentExtractor(HtmlTextExtractor())

    def test_removes_navigation_and_keeps_primary_content(self) -> None:
        html = """
        <html>
          <body>
            <header>Site Navigation</header>
            <nav>Menu Item</nav>
            <main>
              <h1>Repository Title</h1>
              <p>Useful content here.</p>
              <div class="js-repo-nav">Should go away</div>
              <article class="markdown-body" itemprop="text">
                <h2>README</h2>
                <p>Actual README content.</p>
              </article>
            </main>
            <footer>Footer Area</footer>
          </body>
        </html>
        """

        result = self.extractor.extract(html, title="Repository Title")

        lines = result.text.splitlines()
        self.assertIn("Repository Title", lines)
        self.assertIn("Useful content here.", result.text)
        self.assertIn("Actual README content.", result.text)
        self.assertNotIn("Site Navigation", result.text)
        self.assertNotIn("Footer Area", result.text)
        self.assertNotIn("Menu Item", result.text)


class GithubRepoMetadataExtractorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.extractor = extract.GithubRepoMetadataExtractor(HtmlTextExtractor())

    def test_extracts_metadata_from_real_repo_fixture(self) -> None:
        html_path = FIXTURE_ROOT / "repo_v2ray_core.html"
        html = html_path.read_text("utf-8", errors="ignore")

        metadata = self.extractor.extract(html, title="Fixture Repo")
        data = metadata.to_dict()

        self.assertEqual("Fixture Repo", data.get("title"))
        self.assertIsNotNone(metadata.stars.text)
        if metadata.stars.count is not None:
            self.assertGreater(metadata.stars.count, 0)
        self.assertIsNotNone(metadata.forks.text)
        self.assertIsNotNone(metadata.about)
        self.assertIn("A platform for building proxies", metadata.about)
        self.assertIsNotNone(metadata.readme)
        self.assertIn("Move To", metadata.readme)


if __name__ == "__main__":
    unittest.main()
