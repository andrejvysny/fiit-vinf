import json
import tempfile
import unittest
from pathlib import Path

import extract


class ExtractCliTests(unittest.TestCase):
    def test_cli_processes_html_and_writes_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            input_root = tmp / "html"
            input_root.mkdir()
            html_file = input_root / "sample.html"
            html_file.write_text(
                """
                <html>
                  <head><title>Sample Page</title></head>
                  <body>
                    <h1>Hello</h1>
                    <p>MIT License applies.</p>
                    <a href="https://example.com/path">link</a>
                  </body>
                </html>
                """,
                encoding="utf-8",
            )

            text_root = tmp / "text"
            main_root = tmp / "main"
            structured_root = tmp / "structured"
            entities_file = tmp / "entities.tsv"

            exit_code = extract.main(
                [
                    "--input-root",
                    str(input_root),
                    "--output-text",
                    str(text_root),
                    "--output-main-content",
                    str(main_root),
                    "--output-structured",
                    str(structured_root),
                    "--entities-file",
                    str(entities_file),
                    "--limit",
                    "10",
                ]
            )

            self.assertEqual(0, exit_code)

            text_output = text_root / "text_sample.html.txt"
            self.assertTrue(text_output.exists())
            text_content = text_output.read_text(encoding="utf-8")
            self.assertIn("Sample Page", text_content)
            self.assertIn("Hello", text_content)

            main_output = main_root / "main_sample.html.txt"
            self.assertTrue(main_output.exists())
            main_content = main_output.read_text(encoding="utf-8")
            self.assertIn("Hello", main_content)
            self.assertNotIn("Sign in", main_content)

            structured_output = structured_root / "structured_sample.html.json"
            self.assertTrue(structured_output.exists())
            structured_data = json.loads(structured_output.read_text(encoding="utf-8"))
            self.assertEqual("Sample Page", structured_data.get("title"))
            self.assertIsNone(structured_data.get("stars"))
            self.assertIsNone(structured_data.get("forks"))
            self.assertIsNone(structured_data.get("about"))
            self.assertIsNone(structured_data.get("readme"))

            entities_content = entities_file.read_text(encoding="utf-8").strip().splitlines()
            self.assertGreaterEqual(len(entities_content), 2)
            header = entities_content[0]
            self.assertEqual("doc_id\ttype\tvalue\tstart\tend\tmatch", header)
            body_rows = entities_content[1:]
            self.assertTrue(any("LICENSE" in row for row in body_rows))
            self.assertTrue(any("URL" in row for row in body_rows))


if __name__ == "__main__":
    unittest.main()
