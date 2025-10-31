
import logging
from pathlib import Path
from typing import Optional

from extractor.data import ExtractionSummary
from extractor.entity_main import HtmlFileProcessor
from extractor.io_utils import HtmlFileDiscovery
from extractor.outputs import EntitiesOutput

logger = logging.getLogger("extractor.pipeline")

class ExtractorPipeline:
    """Object-oriented coordinator for the extractor workflow."""

    def __init__(
        self,
        input_root: Path,
        *,
        limit: Optional[int],
        force: bool,
        dry_run: bool,
        text_out: Optional[Path],
        entities_out: Optional[Path],
    ):
        self._input_root = input_root.resolve()
        self._limit = limit
        self._force = force
        self._dry_run = dry_run
        self._text_out = text_out
        self._entities_out_path = entities_out

    def run(self) -> int:
        """Execute the extractor pipeline."""

        html_files = HtmlFileDiscovery(self._input_root).discover(limit=self._limit)

        if not html_files:
            logger.warning("No HTML files found in %s", self._input_root)
            return 0

        logger.info("Found %d HTML files to process", len(html_files))

        if self._dry_run:
            for path in html_files:
                print(path)
            return 0

        try:
            entities_output_handler = self._open_entities_output()
        except Exception:
            return 1

        processor = HtmlFileProcessor(
            self._input_root,
            self._text_out,
            force=self._force,
        )
        summary = ExtractionSummary()

        try:
            for index, html_path in enumerate(html_files, start=1):
                try:
                    stats = processor.process(html_path, entities_output_handler)
                    summary.incorporate(stats)
                    if index % 100 == 0 or index == len(html_files):
                        logger.info(
                            "Progress: %d/%d files processed, %d entities extracted",
                            index,
                            len(html_files),
                            summary.entities_extracted,
                        )
                except Exception as exc:
                    logger.exception("Failed to process %s: %s", html_path, exc)
        finally:
            self._close_entities_output(entities_output_handler)

        self._log_summary(summary, entities_output_handler is not None)
        return 0

    def _open_entities_output(self) -> Optional[EntitiesOutput]:
        if not self._entities_out_path:
            return None

        try:
            handler = EntitiesOutput(self._entities_out_path)
            handler.open()
            return handler
        except Exception as exc:
            logger.error("Failed to create entities TSV writer: %s", exc)
            raise

    @staticmethod
    def _close_entities_output(handler: Optional[EntitiesOutput]) -> None:
        if handler:
            try:
                handler.close()
            except Exception as exc:
                logger.error("Failed to close entities TSV writer: %s", exc)

    def _log_summary(self, summary: ExtractionSummary, entities_enabled: bool) -> None:
        logger.info("=" * 70)
        logger.info("EXTRACTION COMPLETE")
        logger.info("=" * 70)
        logger.info("Files processed:      %d", summary.files_processed)
        logger.info("Files skipped:        %d", summary.files_skipped)
        logger.info("Raw text written:     %d", summary.text_written)
        logger.info("Total entities:       %d", summary.entities_extracted)
        logger.info("Stars found:          %d", summary.stars_found)
        logger.info("Forks found:          %d", summary.forks_found)
        logger.info("Lang stats found:     %d", summary.langs_found)
        logger.info("Readmes found:        %d", summary.readme_found)
        logger.info("Licenses found:       %d", summary.license_found)
        logger.info("Topics found:         %d", summary.topics_found)
        logger.info("URLs found:           %d", summary.urls_found)
        logger.info("Emails found:         %d", summary.emails_found)
        logger.info("")
        logger.info("Outputs:")
        if entities_enabled:
            logger.info("  Entities TSV:       %s", self._entities_out_path)
        else:
            logger.info("  Entities TSV:       (disabled)")
        if self._text_out:
            logger.info("  Raw text:           %s", self._text_out)
        else:
            logger.info("  Raw text:           (disabled)")
