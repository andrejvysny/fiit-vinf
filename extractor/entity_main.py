"""Main orchestration for entity extraction pipeline focused on metadata."""

import logging
from pathlib import Path
from typing import Optional

from extractor import entity_extractors, html_clean, io_utils
from extractor.data import FileExtractionStats
from extractor.outputs import EntitiesOutput, text_exists, write_text


logger = logging.getLogger("extractor.entities")



class HtmlFileProcessor:
    """Encapsulates entity extraction for a single HTML file."""

    def __init__(
        self,
        input_root: Path,
        text_out: Optional[Path],
        *,
        force: bool,
    ):
        self._input_root = input_root
        self._text_out = text_out
        self._force = force

    def process(
        self,
        html_path: Path,
        entities_output: Optional[EntitiesOutput],
    ) -> FileExtractionStats:
        """Process a single HTML file and extract entities."""
        stats = FileExtractionStats()
        doc_id = self._get_doc_id(html_path)

        if not self._force:
            text_already_written = text_exists(self._text_out, html_path, doc_id, self._input_root)
            if text_already_written and entities_output is None:
                stats.skipped = True
                return stats

        try:
            html_content = io_utils.read_text_file(html_path, errors='ignore')
        except Exception as exc:
            logger.warning("Failed to read %s: %s", html_path, exc)
            return stats

        if not html_content.strip():
            logger.debug("Empty HTML file: %s", html_path)
            return stats

        raw_text = ""
        try:
            raw_text = html_clean.html_to_text(html_content, strip_boilerplate=False)
        except Exception as exc:
            logger.warning("Failed to extract raw text from %s: %s", html_path, exc)

        try:
            if write_text(
                self._text_out,
                html_path,
                doc_id,
                raw_text,
                self._input_root,
                force=self._force,
            ):
                stats.text_written = True
        except Exception as exc:
            logger.warning("Failed to write raw text for %s: %s", html_path, exc)

        try:
            entities = entity_extractors.extract_all_entities(doc_id, html_content)
            if entities_output:
                try:
                    entities_output.write(entities)
                except Exception as exc:
                    logger.warning("Failed to write entities for %s: %s", html_path, exc)

            stats.entities_extracted += len(entities)
            for row in entities:
                entity_type = row[1]
                if entity_type == 'STAR_COUNT':
                    stats.stars_found = True
                elif entity_type == 'FORK_COUNT':
                    stats.forks_found = True
                elif entity_type == 'LANG_STATS':
                    stats.langs_found = True
                elif entity_type == 'README':
                    stats.readme_found = True
                elif entity_type == 'LICENSE':
                    stats.license_found = True
                elif entity_type == 'TOPICS':
                    stats.topics_found = True
                elif entity_type == 'URL':
                    stats.urls_found = True
                elif entity_type == 'EMAIL':
                    stats.emails_found = True
        except Exception as exc:
            logger.exception("Failed to extract entities from %s: %s", html_path, exc)

        return stats

    @staticmethod
    def _get_doc_id(html_path: Path) -> str:
        """Derive document identifier from file path."""
        return html_path.stem



