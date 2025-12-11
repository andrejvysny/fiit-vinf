import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from extractor import html_clean, entity_extractors
from spark.jobs import html_extractor


class SparkExtractorPartitionTests(unittest.TestCase):
    def test_partition_writes_text_and_entities(self) -> None:
        sample_html = """
        <html>
          <body>
            <span id="repo-stars-counter-star" title="1,234">★</span>
            <a class="topic-tag" href="#">spark-migration</a>
            <a href="https://example.com/page">Example</a>
            Contact: maintainer@example.com
          </body>
        </html>
        """

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            input_root = root / "workspace" / "store" / "html" / "org" / "repo"
            text_out = root / "workspace" / "store" / "text"
            input_root.mkdir(parents=True, exist_ok=True)
            text_out.mkdir(parents=True, exist_ok=True)

            html_path = input_root / "document.html"
            html_path.write_text(sample_html, encoding="utf-8")

            conf = {
                "input_root": str(root / "workspace" / "store" / "html"),
                "text_out": str(text_out),
                "enable_text": True,
                "enable_entities": True,
                "force": True,
                "dry_run": False,
            }
            results = list(html_extractor._process_partition([str(html_path)], SimpleNamespace(value=conf)))
            self.assertEqual(len(results), 1)

            stats, entities = results[0]
            self.assertEqual(stats[0], 1)  # files_processed
            self.assertEqual(stats[2], 1)  # text_written
            self.assertEqual(stats[3], len(entities))  # entity count parity

            expected_text = html_clean.html_to_text(sample_html, strip_boilerplate=False)
            relative = html_path.relative_to(Path(conf["input_root"]))
            target_text = text_out / relative.parent / f"{html_path.stem}.txt"
            self.assertTrue(target_text.exists(), "text file should be written")
            self.assertEqual(target_text.read_text(encoding="utf-8"), expected_text)

            expected_entities = entity_extractors.extract_all_entities(html_path.stem, sample_html)
            self.assertEqual(entities, expected_entities)


if __name__ == "__main__":
    unittest.main()
