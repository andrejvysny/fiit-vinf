import argparse
import html
import json
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple


logger = logging.getLogger("extractor")


@dataclass
class TextExtractionResult:
    """Container for the cleaned text representation of an HTML document."""

    title: str
    text: str


class HtmlTextExtractor:
    """Regex-only HTML to plain-text converter with light boilerplate filtering."""

    _TITLE_RE = re.compile(r"(?is)<title[^>]*>(.*?)</title>")
    _HEAD_RE = re.compile(r"(?is)<head\b[^>]*>.*?</head>")
    _DROP_BLOCK_TAGS_RE = re.compile(
        r"(?is)<(script|style|iframe|canvas|svg|noscript|template|form|dialog)[^>]*>.*?</\1>"
    )
    _NAV_LIKE_TAGS_RE = re.compile(
        r"(?is)<(header|footer|nav|aside)[^>]*>.*?</\1>"
    )
    _COMMENT_RE = re.compile(r"<!--.*?-->", re.DOTALL)
    _DOCTYPE_RE = re.compile(r"<!DOCTYPE.*?>", re.IGNORECASE | re.DOTALL)
    _SELF_CLOSING_METADATA_RE = re.compile(r"(?is)<(meta|link|base|input)[^>]*?>")
    _BLOCK_BREAK_RE = re.compile(
        r"</?(?:p|div|section|article|main|h[1-6]|pre|blockquote|li|ul|ol|table|tr|thead|tbody|tfoot|th|td|dl|dt|dd)[^>]*>",
        re.IGNORECASE,
    )
    _BR_RE = re.compile(r"<br\s*/?>", re.IGNORECASE)
    _HR_RE = re.compile(r"<hr\s*/?>", re.IGNORECASE)
    _TAG_RE = re.compile(r"<[^>]+>")
    _MULTI_SPACE_RE = re.compile(r"[ \t\f\v]+")
    _AROUND_NEWLINE_SPACE_RE = re.compile(r"[ \t\f\v]*\n[ \t\f\v]*")
    _MULTI_NEWLINE_RE = re.compile(r"\n{3,}")

    _ATTRIBUTE_KEYWORD_RE = re.compile(
        r"(?is)<(?P<tag>\w+)(?P<attrs>[^>]*?(?:class|id)\s*=\s*['\"][^'\"]*(?:header|footer|nav|menu|toolbar|sidebar|breadcrumbs|pagination|command-bar|filter-bar)[^'\"]*['\"][^>]*)>.*?</\1>"
    )

    _LINE_DENYLIST = {
        "sign in",
        "sign up",
        "pricing",
        "product",
        "solutions",
        "security",
        "issues",
        "pull requests",
        "pull request",
        "actions",
        "projects",
        "wiki",
        "marketplace",
        "explore",
        "sponsors",
        "codespaces",
        "insights",
        "compare",
        "security policy",
        "github",
    }

    def extract(self, html_content: str) -> TextExtractionResult:
        if not html_content:
            return TextExtractionResult(title="", text="")

        working = html_content
        title = ""

        # Extract title before removing head
        title_match = self._TITLE_RE.search(working)
        if title_match:
            title_raw = html.unescape(title_match.group(1).strip())
            title = self._MULTI_SPACE_RE.sub(" ", title_raw)

        working = self._strip_iteratively(working, self._HEAD_RE)
        working = self._strip_iteratively(working, self._DROP_BLOCK_TAGS_RE)
        working = self._strip_iteratively(working, self._NAV_LIKE_TAGS_RE)
        working = self._strip_iteratively(working, self._ATTRIBUTE_KEYWORD_RE)
        working = self._SELF_CLOSING_METADATA_RE.sub(" ", working)
        working = self._COMMENT_RE.sub(" ", working)
        working = self._DOCTYPE_RE.sub(" ", working)

        working = self._BR_RE.sub("\n", working)
        working = self._HR_RE.sub("\n", working)
        working = self._BLOCK_BREAK_RE.sub("\n", working)

        working = self._TAG_RE.sub(" ", working)

        working = html.unescape(working)
        working = working.replace("\xa0", " ")
        working = working.replace("\r\n", "\n").replace("\r", "\n")

        working = self._MULTI_SPACE_RE.sub(" ", working)
        working = self._AROUND_NEWLINE_SPACE_RE.sub("\n", working)
        working = self._MULTI_NEWLINE_RE.sub("\n\n", working)

        lines = [line.strip() for line in working.split("\n")]
        cleaned_lines: List[str] = [
            line for line in lines if self._should_keep_line(line)
        ]
        text = "\n".join(cleaned_lines).strip()

        return TextExtractionResult(title=title, text=text)

    def extract_text(self, html_content: str) -> str:
        """Extract only the text content without title metadata."""
        return self.extract(html_content).text

    def _strip_iteratively(self, text: str, pattern: re.Pattern) -> str:
        while True:
            text, count = pattern.subn(" ", text)
            if count == 0:
                return text

    def _should_keep_line(self, line: str) -> bool:
        if not line:
            return False
        lower = line.lower()
        if lower in self._LINE_DENYLIST:
            return False
        if not any(char.isalnum() for char in lower):
            return False
        return True


def load_file_list(path: Path) -> List[str]:
    entries: List[str] = []
    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            entries.append(line)
    return entries


def resolve_html_paths(
    input_root: Path,
    explicit_files: Sequence[str],
    list_file: Optional[Path],
    limit: Optional[int],
) -> List[Path]:
    candidates: List[Path] = []

    if list_file:
        if not list_file.exists():
            raise FileNotFoundError(f"Files list not found: {list_file}")
        candidates.extend(Path(entry) for entry in load_file_list(list_file))

    if explicit_files:
        candidates.extend(Path(entry) for entry in explicit_files)

    paths: List[Path] = []
    if candidates:
        for candidate in candidates:
            path = candidate
            if not path.is_absolute():
                path = input_root / path
            if path.suffix.lower() not in {".html", ".htm"}:
                logger.debug("Skipping non-HTML entry: %s", path)
                continue
            if path.exists():
                paths.append(path.resolve())
            else:
                logger.warning("Declared HTML file missing: %s", path)
    else:
        if not input_root.exists():
            logger.error("Input root does not exist: %s", input_root)
            return []
        paths = sorted(input_root.rglob("*.html"))

    if limit is not None and limit >= 0:
        paths = paths[:limit]

    return paths


def load_crawl_metadata_index(metadata_path: Path) -> Dict[Path, str]:
    """Load crawl metadata jsonl and return mapping from stored_path (resolved Path) to URL.

    Best-effort: we resolve stored_path entries and return only those that parse correctly.
    """
    index: Dict[Path, str] = {}
    if not metadata_path.exists():
        return index

    try:
        with metadata_path.open("r", encoding="utf-8") as handle:
            for raw in handle:
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    obj = json.loads(raw)
                except Exception:
                    # skip malformed lines
                    continue

                stored = obj.get("stored_path")
                url = obj.get("url")
                if not stored or not url:
                    continue
                try:
                    p = Path(stored).resolve()
                except Exception:
                    # fallback to using path as-is
                    try:
                        p = Path(stored)
                    except Exception:
                        continue

                index[p] = url
    except Exception:
        # defensive: if anything goes wrong, return whatever we collected
        return index

    return index


def ensure_parent(path: Path) -> None:
    parent = path.parent
    if not parent.exists():
        parent.mkdir(parents=True, exist_ok=True)


def mirror_output_path(input_root: Path, output_root: Path, html_path: Path) -> Path:
    relative = html_path.relative_to(input_root)
    target = output_root / relative
    return target.with_suffix(".txt")


def extract_file(
    html_path: Path,
    input_root: Path,
    output_root: Path,
    extractor: HtmlTextExtractor,
    *,
    force: bool = False,
) -> Tuple[bool, Path]:
    target_path = mirror_output_path(input_root, output_root, html_path)
    if not force and target_path.exists():
        return False, target_path

    html_content = html_path.read_text("utf-8", errors="ignore")
    result = extractor.extract(html_content)
    ensure_parent(target_path)
    target_path.write_text(result.text, encoding="utf-8")
    return True, target_path


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Regex-only HTML content extractor."
    )
    parser.add_argument(
        "--input-root",
        default="workspace/store/html",
        help="Root directory containing downloaded HTML files (default: %(default)s)",
    )
    parser.add_argument(
        "--output-root",
        default="workspace/store/text",
        help="Destination directory for extracted text (default: %(default)s)",
    )
    parser.add_argument(
        "--files-list",
        help=(
            "Optional path to a text file listing HTML files to process "
            "(one per line, absolute or relative to --input-root)."
        ),
    )
    parser.add_argument(
        "--file",
        action="append",
        dest="files",
        default=[],
        help="Explicit HTML file to process (can be provided multiple times).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Maximum number of files to process (after applying filters).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Reprocess files even when output text already exists.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List files that would be processed without writing outputs.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging output.",
    )
    parser.add_argument(
        "--metadata",
        default="workspace/metadata/crawl_metadata.jsonl",
        help="Path to crawl metadata jsonl (default: %(default)s)",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    input_root = Path(args.input_root).resolve()
    output_root = Path(args.output_root).resolve()
    files_list_path = Path(args.files_list).resolve() if args.files_list else None

    html_paths = resolve_html_paths(
        input_root=input_root,
        explicit_files=args.files,
        list_file=files_list_path,
        limit=args.limit,
    )

    if not html_paths:
        logger.warning("No HTML files selected for extraction.")
        return 0

    logger.info("Selected %d HTML files for extraction", len(html_paths))

    if args.dry_run:
        for path in html_paths:
            print(path)
        return 0

    # Load metadata index and prepare TSV output (best-effort matching)
    metadata_path = Path(args.metadata).resolve()
    metadata_index = load_crawl_metadata_index(metadata_path)
    filename_index: Dict[str, str] = {p.name: url for p, url in metadata_index.items()}

    tsv_path = metadata_path.parent / "extracted.tsv"
    if not tsv_path.parent.exists():
        tsv_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        tsv_handle = tsv_path.open("w", encoding="utf-8")
    except Exception:
        logger.exception("Failed to open TSV output: %s", tsv_path)
        return 1

    # TSV header
    tsv_handle.write("html_path\textracted_path\turl\n")

    extractor = HtmlTextExtractor()
    processed = 0
    skipped = 0

    for index, html_path in enumerate(html_paths, start=1):
        try:
            wrote, target_path = extract_file(
                html_path,
                input_root,
                output_root,
                extractor,
                force=args.force,
            )
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.exception("Failed to extract %s: %s", html_path, exc)
            # still attempt to record mapping; assume target based on mirroring
            wrote = False
            target_path = mirror_output_path(input_root, output_root, html_path)

        if wrote:
            processed += 1
            logger.debug("Extracted %s -> %s", html_path, target_path)
        else:
            skipped += 1

        # Progress logging
        if processed % 100 == 0 or index == len(html_paths):
            logger.info(
                "Progress: %d processed, %d skipped (of %d)",
                processed,
                skipped,
                len(html_paths),
            )

        # Resolve URL: exact path match first, then filename fallback
        url = ""
        try:
            resolved = html_path.resolve()
            url = metadata_index.get(resolved, "")
        except Exception:
            url = ""

        if not url:
            url = filename_index.get(html_path.name, "")

        if url:
            url = url.replace("\t", " ").replace("\n", " ")

        # Write TSV row (absolute paths)
        try:
            tsv_handle.write(f"{str(html_path.resolve())}\t{str(target_path.resolve())}\t{url}\n")
        except Exception:
            logger.exception("Failed to write TSV row for %s", html_path)

    logger.info(
        "Extraction complete: %d processed, %d skipped. Output root: %s",
        processed,
        skipped,
        output_root,
    )

    try:
        tsv_handle.close()
    except Exception:
        logger.exception("Failed to close TSV file: %s", tsv_path)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
