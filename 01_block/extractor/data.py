from dataclasses import dataclass


@dataclass
class FileExtractionStats:
    """Per-file extraction statistics."""
    entities_extracted: int = 0
    stars_found: bool = False
    forks_found: bool = False
    langs_found: bool = False
    text_written: bool = False
    skipped: bool = False
    readme_found: bool = False
    license_found: bool = False
    topics_found: bool = False
    urls_found: bool = False
    emails_found: bool = False



@dataclass
class ExtractionSummary:
    """Aggregate extraction statistics."""

    files_processed: int = 0
    files_skipped: int = 0
    text_written: int = 0
    entities_extracted: int = 0
    stars_found: int = 0
    forks_found: int = 0
    langs_found: int = 0
    readme_found: int = 0
    license_found: int = 0
    topics_found: int = 0
    urls_found: int = 0
    emails_found: int = 0

    def incorporate(self, stats: FileExtractionStats) -> None:
        """Update aggregate counters from a per-file result."""
        if stats.skipped:
            self.files_skipped += 1
        else:
            self.files_processed += 1

        self.entities_extracted += stats.entities_extracted
        self.text_written += int(stats.text_written)
        self.stars_found += int(stats.stars_found)
        self.forks_found += int(stats.forks_found)
        self.langs_found += int(stats.langs_found)
        self.readme_found += int(stats.readme_found)
        self.license_found += int(stats.license_found)
        self.topics_found += int(stats.topics_found)
        self.urls_found += int(stats.urls_found)
        self.emails_found += int(stats.emails_found)
