import json
import time
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from crawler.config import (
    CrawlerScraperConfig,
    RobotsConfig,
    ScopeConfig,
    LimitsConfig,
    CapsConfig,
    StorageConfig,
    LogsConfig,
    SleepConfig,
)
from crawler.service import CrawlerScraperService


def make_config(workspace: str) -> CrawlerScraperConfig:
    return CrawlerScraperConfig(
        run_id="test-run",
        workspace=workspace,
        user_agent="TestAgent/1.0",
        robots=RobotsConfig(user_agent="TestAgent/1.0"),
        scope=ScopeConfig(allowed_hosts=["github.com"]),
        limits=LimitsConfig(),
        caps=CapsConfig(),
        storage=StorageConfig(
            frontier_file="state/frontier.txt",
            fetched_urls_file="state/fetched.txt",
            robots_cache_file="state/robots.jsonl",
            metadata_file="metadata/docs.tsv",
            html_store_root="store/html",
        ),
        logs=LogsConfig(log_file="logs/crawler.log", log_level="INFO"),
        sleep=SleepConfig(),
    )


class CrawlerServiceUtilityTests(unittest.TestCase):
    def test_html_statistics_helpers(self) -> None:
        with TemporaryDirectory() as tmpdir:
            config = make_config(tmpdir)
            service = CrawlerScraperService(config)

            html_dir = Path(tmpdir) / "store" / "html" / "aa"
            html_dir.mkdir(parents=True, exist_ok=True)
            (html_dir / "doc1.html").write_text("abc", encoding="utf-8")
            (html_dir / "doc2.html").write_text("abcdef", encoding="utf-8")
            (html_dir / "ignore.txt").write_text("12345", encoding="utf-8")

            self.assertEqual(2, service._calculate_html_files_count())
            self.assertEqual(3 + 6, service._calculate_html_storage_bytes())

    def test_acceptance_rate_and_format(self) -> None:
        with TemporaryDirectory() as tmpdir:
            service = CrawlerScraperService(make_config(tmpdir))
            service.stats["urls_enqueued"] = 8
            service.stats["policy_denied"] = 2

            self.assertAlmostEqual(80.0, service._get_acceptance_rate())
            self.assertEqual("500 B", service._format_bytes(500))
            self.assertEqual("1.00 KB", service._format_bytes(1024))
            self.assertEqual("1.00 MB", service._format_bytes(1024 * 1024))

    def test_persist_service_stats_writes_file(self) -> None:
        with TemporaryDirectory() as tmpdir:
            service = CrawlerScraperService(make_config(tmpdir))

            html_dir = Path(tmpdir) / "store" / "html"
            html_dir.mkdir(parents=True, exist_ok=True)
            (html_dir / "doc.html").write_text("data", encoding="utf-8")

            stats_path = Path(tmpdir) / "state_stats.json"
            service._stats_path = stats_path
            service.start_time = time.time() - 5
            service.stats["urls_enqueued"] = 5
            service.stats["policy_denied"] = 5
            service.stats["urls_fetched"] = 10

            service._persist_service_stats()

            payload = json.loads(stats_path.read_text(encoding="utf-8"))
            self.assertEqual(1, payload["html_files_count"])
            self.assertEqual(4, payload["html_storage_bytes"])
            self.assertIn("acceptance_rate_percent", payload)
            self.assertGreaterEqual(payload["runtime_seconds"], 0)


if __name__ == "__main__":
    unittest.main()
