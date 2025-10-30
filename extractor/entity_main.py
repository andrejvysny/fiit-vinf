"""Main orchestration for entity extraction pipeline.

This module orchestrates the full extraction process:
1. Read HTML files
2. Extract preprocessed text
3. Extract all entity types
4. Write entities.tsv
5. Write text-preprocessed files
6. Optionally write README files

Integrates with existing extractor CLI.
"""

import argparse
import logging
from pathlib import Path
from typing import Optional, Sequence

from extractor import entity_extractors, html_clean, io_utils


logger = logging.getLogger("extractor.entities")


def get_doc_id_from_path(html_path: Path) -> str:
    """Extract document ID from HTML file path.

    Uses the file stem (name without extension) as doc_id.

    Args:
        html_path: Path to HTML file

    Returns:
        Document ID string
    """
    return html_path.stem


def process_html_file(
    html_path: Path,
    entities_writer: Optional[io_utils.TSVWriter],
    text_out: Optional[Path],
    preproc_out: Optional[Path],
    readme_out: Optional[Path],
    input_root: Path,
    force: bool = False,
) -> dict:
    """Process a single HTML file and extract entities.

    Args:
        html_path: Path to HTML file
        entities_writer: TSV writer for entities (or None if dry-run)
        text_out: Output directory for raw text (or None to skip)
        preproc_out: Output directory for preprocessed text (or None to skip)
        readme_out: Output directory for README files (or None to skip)
        input_root: Input root directory (for preserving structure)
        force: If True, overwrite existing files

    Returns:
        Dict with statistics about extraction
    """
    stats = {
        'entities_extracted': 0,
        'readme_found': False,
        'stars_found': False,
        'forks_found': False,
        'langs_found': False,
        'text_written': False,
        'preproc_written': False,
        'skipped': False,
    }

    # Derive doc_id
    doc_id = get_doc_id_from_path(html_path)

    # Check if files already exist (unless force=True)
    if not force:
        rel_path = html_path.relative_to(input_root)

        # Check raw text file
        text_exists = False
        if text_out:
            text_file = text_out / rel_path.parent / f"{doc_id}.txt"
            text_exists = text_file.exists()

        # Check preprocessed text file
        preproc_exists = False
        if preproc_out:
            preproc_file = preproc_out / rel_path.parent / f"{doc_id}.txt"
            preproc_exists = preproc_file.exists()

        # If both exist, skip processing (entities are cheap to re-extract)
        if text_exists and preproc_exists:
            stats['skipped'] = True
            return stats

    # Read HTML
    try:
        html_content = io_utils.read_text_file(html_path, errors='ignore')
    except Exception as exc:
        logger.warning("Failed to read %s: %s", html_path, exc)
        return stats

    if not html_content.strip():
        logger.debug("Empty HTML file: %s", html_path)
        return stats

    # Extract raw text (just remove HTML tags, no boilerplate removal)
    raw_text = ""
    try:
        raw_text = html_clean.html_to_text(html_content, strip_boilerplate=False)
    except Exception as exc:
        logger.warning("Failed to extract raw text from %s: %s", html_path, exc)

    # Write raw text if output directory specified
    if text_out and raw_text:
        try:
            # Mirror directory structure
            rel_path = html_path.relative_to(input_root)
            text_file = text_out / rel_path.parent / f"{doc_id}.txt"
            if force or not text_file.exists():
                io_utils.write_text_file(text_file, raw_text)
                stats['text_written'] = True
        except Exception as exc:
            logger.warning("Failed to write raw text for %s: %s", html_path, exc)

    # Extract preprocessed text (with boilerplate removal)
    preprocessed_text = ""
    try:
        preprocessed_text = html_clean.html_to_text(html_content, strip_boilerplate=True)
    except Exception as exc:
        logger.warning("Failed to preprocess %s: %s", html_path, exc)

    # Write preprocessed text if output directory specified
    if preproc_out and preprocessed_text:
        try:
            # Mirror directory structure
            rel_path = html_path.relative_to(input_root)
            preproc_file = preproc_out / rel_path.parent / f"{doc_id}.txt"
            if force or not preproc_file.exists():
                io_utils.write_text_file(preproc_file, preprocessed_text)
                stats['preproc_written'] = True
        except Exception as exc:
            logger.warning("Failed to write preprocessed text for %s: %s", html_path, exc)

    # Extract README separately (to write to dedicated readme directory)
    readme_text = None
    try:
        readme_text, readme_entities = entity_extractors.extract_readme(doc_id, html_content)
        if readme_text:
            stats['readme_found'] = True

            # Write README file to dedicated readme directory
            if readme_out:
                try:
                    rel_path = html_path.relative_to(input_root)
                    # Save in readme directory with same structure
                    readme_file = readme_out / rel_path.parent / f"{doc_id}.txt"
                    io_utils.write_text_file(readme_file, readme_text)
                except Exception as exc:
                    logger.warning("Failed to write README for %s: %s", html_path, exc)

            # Write README entities
            if entities_writer:
                try:
                    entities_writer.write_rows(readme_entities)
                    stats['entities_extracted'] += len(readme_entities)
                except Exception as exc:
                    logger.warning("Failed to write README entities for %s: %s", html_path, exc)

    except Exception as exc:
        logger.warning("Failed to extract README from %s: %s", html_path, exc)

    # Extract all other entities
    try:
        entities = entity_extractors.extract_all_entities(doc_id, html_content, preprocessed_text)

        # Write entities
        if entities_writer:
            entities_writer.write_rows(entities)

        stats['entities_extracted'] += len(entities)

        # Track specific entity types for stats
        for row in entities:
            entity_type = row[1]
            if entity_type == 'STAR_COUNT':
                stats['stars_found'] = True
            elif entity_type == 'FORK_COUNT':
                stats['forks_found'] = True
            elif entity_type == 'LANG_STATS':
                stats['langs_found'] = True

    except Exception as exc:
        logger.exception("Failed to extract entities from %s: %s", html_path, exc)

    return stats


def parse_entity_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Parse command-line arguments for entity extraction.

    Args:
        argv: Command-line arguments (defaults to sys.argv)

    Returns:
        Parsed arguments namespace
    """
    parser = argparse.ArgumentParser(
        description="Extract entities and preprocessed text from HTML files."
    )

    parser.add_argument(
        '--in',
        '--input',
        dest='input_root',
        default='workspace/store/html',
        help='Input directory containing HTML files (default: %(default)s)',
    )

    parser.add_argument(
        '--text-out',
        dest='text_out',
        default='workspace/store/text',
        help='Output directory for plain text (default: %(default)s)',
    )

    parser.add_argument(
        '--entities-out',
        dest='entities_out',
        default='workspace/store/entities/entities.tsv',
        help='Output TSV file for entities (default: %(default)s)',
    )

    parser.add_argument(
        '--preproc-out',
        dest='preproc_out',
        default='workspace/store/text-preprocessed',
        help='Output directory for preprocessed text (default: %(default)s)',
    )

    parser.add_argument(
        '--readme-out',
        dest='readme_out',
        default='workspace/store/readme',
        help='Output directory for README files (default: %(default)s)',
    )

    parser.add_argument(
        '--limit',
        type=int,
        help='Maximum number of files to process',
    )

    parser.add_argument(
        '--sample',
        type=int,
        help='Process only N sample files for testing (alias for --limit)',
    )

    parser.add_argument(
        '--force',
        action='store_true',
        help='Force reprocessing of all files (overwrite existing outputs)',
    )

    parser.add_argument(
        '--no-text',
        action='store_true',
        help='Skip raw text extraction (disable workspace/store/text/)',
    )

    parser.add_argument(
        '--no-preproc',
        action='store_true',
        help='Skip preprocessed text extraction (disable workspace/store/text-preprocessed/)',
    )

    parser.add_argument(
        '--no-entities',
        action='store_true',
        help='Skip entity extraction (disable workspace/store/entities/)',
    )

    parser.add_argument(
        '--no-readme',
        action='store_true',
        help='Skip README extraction (disable workspace/store/readme/)',
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='List files without processing',
    )

    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable debug logging',
    )

    return parser.parse_args(argv)


def main_entity_extraction(argv: Optional[Sequence[str]] = None) -> int:
    """Main entry point for entity extraction.

    Args:
        argv: Command-line arguments

    Returns:
        Exit code (0 for success)
    """
    args = parse_entity_args(argv)

    # Setup logging
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    )

    # Resolve paths
    input_root = Path(args.input_root).resolve()

    # Disable outputs based on flags
    text_out = None if args.no_text else Path(args.text_out).resolve()
    entities_out = None if args.no_entities else Path(args.entities_out).resolve()
    preproc_out = None if args.no_preproc else Path(args.preproc_out).resolve()
    readme_out = None if args.no_readme else Path(args.readme_out).resolve()

    # Use --sample if provided, otherwise --limit
    limit = args.sample if args.sample else args.limit

    # Discover HTML files
    html_files = io_utils.discover_html_files(input_root, limit=limit)

    if not html_files:
        logger.warning("No HTML files found in %s", input_root)
        return 0

    logger.info("Found %d HTML files to process", len(html_files))

    if args.dry_run:
        for path in html_files:
            print(path)
        return 0

    # Initialize TSV writer
    entities_writer = None
    if entities_out:
        try:
            entities_writer = io_utils.TSVWriter(
                entities_out,
                header=['doc_id', 'type', 'value', 'offsets_json']
            )
            entities_writer.__enter__()
        except Exception as exc:
            logger.error("Failed to create entities TSV writer: %s", exc)
            return 1

    # Process files
    total_stats = {
        'files_processed': 0,
        'files_skipped': 0,
        'text_written': 0,
        'preproc_written': 0,
        'entities_extracted': 0,
        'readme_found': 0,
        'stars_found': 0,
        'forks_found': 0,
        'langs_found': 0,
    }

    try:
        for index, html_path in enumerate(html_files, start=1):
            try:
                stats = process_html_file(
                    html_path,
                    entities_writer,
                    text_out,
                    preproc_out,
                    readme_out,
                    input_root,
                    force=args.force,
                )

                if stats['skipped']:
                    total_stats['files_skipped'] += 1
                else:
                    total_stats['files_processed'] += 1

                total_stats['entities_extracted'] += stats['entities_extracted']
                if stats['text_written']:
                    total_stats['text_written'] += 1
                if stats['preproc_written']:
                    total_stats['preproc_written'] += 1
                if stats['readme_found']:
                    total_stats['readme_found'] += 1
                if stats['stars_found']:
                    total_stats['stars_found'] += 1
                if stats['forks_found']:
                    total_stats['forks_found'] += 1
                if stats['langs_found']:
                    total_stats['langs_found'] += 1

                # Progress logging
                if index % 100 == 0 or index == len(html_files):
                    logger.info(
                        "Progress: %d/%d files processed, %d entities extracted",
                        index,
                        len(html_files),
                        total_stats['entities_extracted']
                    )

            except Exception as exc:
                logger.exception("Failed to process %s: %s", html_path, exc)

    finally:
        # Close TSV writer
        if entities_writer:
            try:
                entities_writer.__exit__(None, None, None)
            except Exception as exc:
                logger.error("Failed to close entities TSV writer: %s", exc)

    # Final summary
    logger.info("=" * 70)
    logger.info("EXTRACTION COMPLETE")
    logger.info("=" * 70)
    logger.info("Files processed:      %d", total_stats['files_processed'])
    logger.info("Files skipped:        %d", total_stats['files_skipped'])
    logger.info("Raw text written:     %d", total_stats['text_written'])
    logger.info("Preprocessed written: %d", total_stats['preproc_written'])
    logger.info("Total entities:       %d", total_stats['entities_extracted'])
    logger.info("README found:         %d", total_stats['readme_found'])
    logger.info("Stars found:          %d", total_stats['stars_found'])
    logger.info("Forks found:          %d", total_stats['forks_found'])
    logger.info("Lang stats found:     %d", total_stats['langs_found'])
    logger.info("")
    logger.info("Outputs:")
    if entities_out:
        logger.info("  Entities TSV:       %s", entities_out)
    else:
        logger.info("  Entities TSV:       (disabled)")
    if text_out:
        logger.info("  Raw text:           %s", text_out)
    else:
        logger.info("  Raw text:           (disabled)")
    if preproc_out:
        logger.info("  Preprocessed text:  %s", preproc_out)
    else:
        logger.info("  Preprocessed text:  (disabled)")
    if readme_out:
        logger.info("  README files:       %s", readme_out)
    else:
        logger.info("  README files:       (disabled)")

    return 0


if __name__ == '__main__':
    raise SystemExit(main_entity_extraction())
