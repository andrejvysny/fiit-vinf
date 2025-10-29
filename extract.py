import argparse
import json
import logging
import re
from pathlib import Path
from dataclasses import dataclass, field
from typing import Iterable, List, Optional, Sequence

from crawler.extractor import (
    EntityMatch,
    HtmlTextExtractor,
    RegexEntityExtractor,
    TextExtractionResult,
)
from bs4 import BeautifulSoup
from bs4.element import NavigableString, Tag


logger = logging.getLogger("extract")


@dataclass
class RepoCounter:
    text: Optional[str] = None
    count: Optional[int] = None

    def to_dict(self) -> Optional[dict]:
        if self.text is None and self.count is None:
            return None
        return {
            "text": self.text,
            "count": self.count,
        }


@dataclass
class RepoMetadata:
    title: Optional[str] = None
    stars: RepoCounter = field(default_factory=RepoCounter)
    forks: RepoCounter = field(default_factory=RepoCounter)
    about: Optional[str] = None
    readme: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "title": self.title,
            "stars": self.stars.to_dict(),
            "forks": self.forks.to_dict(),
            "about": self.about,
            "readme": self.readme,
        }


class GithubMainContentExtractor:
    """Remove GitHub boilerplate (navbars, sidebars, footer) and return main text."""

    _REMOVAL_TAGS = {"nav", "header", "footer", "aside", "form", "dialog"}
    _ROLE_REMOVALS = {"banner", "navigation", "search", "complementary", "contentinfo"}
    _CLASS_KEYWORDS = (
        "header",
        "nav",
        "footer",
        "sidebar",
        "subnav",
        "toolbar",
        "menu",
        "filter-bar",
        "command-bar",
        "command-palette",
        "pagination",
        "octobox",
        "flash",
        "toast",
        "overlay",
        "popover",
        "tooltip",
        "breadcrumbs",
        "marketing",
        "js-repo-nav",
        "js-sidenav",
        "AppHeader",
        "HeaderMenu",
        "ActionList",
        "UnderlineNav",
        "js-snippet-clipboard-copy-header",
    )
    _ID_KEYWORDS = (
        "header",
        "nav",
        "footer",
        "sidebar",
        "login",
        "aria-",
        "readme-toc",
        "repository-container-header",
        "repos-file-tree",
        "ref-selector",
        "branch-selector",
        "code-tab",
        "issues-tab",
        "pull-requests-tab",
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
        "code",
        "marketplace",
        "explore",
        "sponsors",
        "sponsor",
        "codespaces",
        "insights",
        "compare",
        "security policy",
    }

    def __init__(self, text_extractor: Optional[HtmlTextExtractor] = None) -> None:
        self._text_extractor = text_extractor or HtmlTextExtractor()

    def extract(self, html_content: str, *, title: Optional[str] = None) -> TextExtractionResult:
        if not html_content:
            return TextExtractionResult(title=title or "", text="")

        filtered_html = self._filter_html(html_content)
        result = self._text_extractor.extract(filtered_html)

        lines = []
        for line in result.text.splitlines():
            normalised = line.strip()
            if not normalised:
                continue
            if normalised.lower() in self._LINE_DENYLIST:
                continue
            lines.append(normalised)

        text = "\n".join(lines)
        final_title = title if title is not None else result.title
        return TextExtractionResult(title=final_title or "", text=text)

    def _filter_html(self, html_content: str) -> str:
        soup = BeautifulSoup(html_content, "html.parser")
        container: Tag
        if soup.main:
            container = soup.main
        elif soup.body:
            container = soup.body
        else:
            container = soup

        self._strip_elements(container)

        return str(container)

    def _strip_elements(self, node: Tag) -> None:
        for tag_name in self._REMOVAL_TAGS:
            for element in list(node.find_all(tag_name)):
                element.decompose()

        for element in list(
            node.find_all(
                attrs={
                    "role": lambda value: value
                    and any(role in value.lower() for role in self._ROLE_REMOVALS)
                }
            )
        ):
            element.decompose()

        for element in list(node.find_all(True)):
            if element is None or not hasattr(element, "get"):
                continue
            attrs = getattr(element, "attrs", {}) or {}
            if self._should_keep(element):
                continue
            if self._matches_keyword(attrs.get("class"), self._CLASS_KEYWORDS):
                element.decompose()
                continue
            if self._matches_keyword([attrs.get("id")], self._ID_KEYWORDS):
                element.decompose()
                continue
            data_test_selector = attrs.get("data-test-selector") or ""
            if self._matches_keyword([data_test_selector], self._CLASS_KEYWORDS):
                element.decompose()

    def _matches_keyword(self, values: Optional[Iterable[str]], keywords: Sequence[str]) -> bool:
        if not values:
            return False
        for value in values:
            if not value:
                continue
            lower_value = value.lower()
            for keyword in keywords:
                if keyword.lower() in lower_value:
                    return True
        return False

    def _should_keep(self, element: Tag) -> bool:
        if element is None or not hasattr(element, "get"):
            return False
        attrs = getattr(element, "attrs", {}) or {}
        classes = attrs.get("class") or []
        for cls in classes:
            lower = cls.lower()
            if "markdown" in lower or "entry-content" in lower:
                return True
        if element.name == "article" and attrs.get("itemprop") == "text":
            return True
        return False


class GithubRepoMetadataExtractor:
    """Parse repo-specific metadata such as stars, forks, about, and README."""

    _COUNTER_SELECTORS = {
        "stars": [
            "#repo-stars-counter-star",
            "a[href$='/stargazers'] strong",
            "a[href$='/stargazers'] span.Counter",
        ],
        "forks": [
            "#repo-network-counter",
            "a[href$='/forks'] strong",
            "a[href$='/network/members'] strong",
            "a[href$='/forks'] span.Counter",
        ],
    }

    _COUNTER_NORMALISER = re.compile(r"[^0-9kmgb\.]+", re.IGNORECASE)

    def __init__(self, text_extractor: Optional[HtmlTextExtractor] = None) -> None:
        self._text_extractor = text_extractor or HtmlTextExtractor()

    def extract(self, html_content: str, *, title: Optional[str] = None) -> RepoMetadata:
        metadata = RepoMetadata(title=title)
        if not html_content:
            return metadata

        try:
            soup = BeautifulSoup(html_content, "html.parser")
        except Exception as exc:  # pragma: no cover - extremely rare
            logger.debug("BeautifulSoup failed to parse HTML: %s", exc)
            return metadata

        stars_text = self._extract_counter_text(soup, "stars")
        forks_text = self._extract_counter_text(soup, "forks")
        metadata.stars = RepoCounter(
            text=stars_text, count=self._parse_human_count(stars_text)
        )
        metadata.forks = RepoCounter(
            text=forks_text, count=self._parse_human_count(forks_text)
        )
        metadata.about = self._extract_about(soup)
        metadata.readme = self._extract_readme(soup)

        return metadata

    def _extract_counter_text(self, soup: BeautifulSoup, key: str) -> Optional[str]:
        selectors = self._COUNTER_SELECTORS.get(key, [])
        for selector in selectors:
            element = soup.select_one(selector)
            if element:
                text = element.get_text(strip=True)
                if text:
                    return text
        return None

    def _parse_human_count(self, text: Optional[str]) -> Optional[int]:
        if not text:
            return None
        cleaned = text.strip().lower().replace(",", "")
        cleaned = self._COUNTER_NORMALISER.sub("", cleaned)
        if not cleaned:
            return None

        multiplier = 1
        if cleaned.endswith("k"):
            multiplier = 1_000
            cleaned = cleaned[:-1]
        elif cleaned.endswith("m"):
            multiplier = 1_000_000
            cleaned = cleaned[:-1]
        elif cleaned.endswith("b"):
            multiplier = 1_000_000_000
            cleaned = cleaned[:-1]

        try:
            value = float(cleaned)
        except ValueError:
            return None

        return int(round(value * multiplier))

    def _extract_about(self, soup: BeautifulSoup) -> Optional[str]:
        headings = soup.find_all(
            lambda tag: tag.name in {"h1", "h2", "h3"}
            and tag.get_text(strip=True).lower() == "about"
        )
        if not headings:
            return None

        heading = headings[0]
        lines: List[str] = []
        for sibling in heading.next_siblings:
            if isinstance(sibling, NavigableString):
                text = sibling.strip()
                if text:
                    lines.append(text)
                continue
            if isinstance(sibling, Tag):
                if sibling.name in {"h1", "h2", "h3"}:
                    break
                text = sibling.get_text(" \n", strip=True)
                if text:
                    lines.append(text)
        if not lines:
            return None
        return "\n".join(dict.fromkeys(lines))

    def _extract_readme(self, soup: BeautifulSoup) -> Optional[str]:
        article = soup.select_one("article.markdown-body, article[itemprop='text']")
        if article:
            html_fragment = str(article)
            return self._normalise_text(html_fragment)

        json_readme = self._extract_readme_from_embedded_json(soup)
        if json_readme:
            return json_readme

        return None

    def _normalise_text(self, html_fragment: str) -> str:
        result = self._text_extractor.extract(html_fragment)
        return result.text

    def _extract_readme_from_embedded_json(self, soup: BeautifulSoup) -> Optional[str]:
        for script in soup.find_all("script", attrs={"type": "application/json"}):
            data = script.string
            if not data:
                continue
            if "overviewFiles" not in data and "readme" not in data:
                continue
            try:
                payload = json.loads(data)
            except json.JSONDecodeError:
                continue

            readme_html = self._search_readme_in_payload(payload)
            if readme_html:
                return self._normalise_text(readme_html)
        return None

    def _search_readme_in_payload(self, payload: dict) -> Optional[str]:
        queue: List = [payload]
        while queue:
            current = queue.pop()
            if isinstance(current, dict):
                if "richText" in current and current.get("tabName") == "README":
                    return current["richText"]
                if "readme" in current and isinstance(current["readme"], dict):
                    text_html = current["readme"].get("html")
                    if text_html:
                        return text_html
                    rich_text = current["readme"].get("richText")
                    if rich_text:
                        return rich_text
                queue.extend(current.values())
            elif isinstance(current, list):
                queue.extend(current)
        return None


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Regex-only HTML text extractor and entity annotator"
    )
    parser.add_argument(
        "--input-root",
        default="workspace/store/html",
        help="Root directory containing crawled HTML files (default: %(default)s)",
    )
    parser.add_argument(
        "--output-text",
        default="workspace/data/text",
        help="Directory where extracted text files will be written (default: %(default)s)",
    )
    parser.add_argument(
        "--output-main-content",
        dest="output_main_content",
        default="workspace/data/main_content",
        help="Directory for cleaned main-content text outputs (default: %(default)s)",
    )
    parser.add_argument(
        "--output-structured",
        dest="output_structured",
        default="workspace/data/structured",
        help="Directory for structured repository metadata outputs (default: %(default)s)",
    )
    parser.add_argument(
        "--entities-file",
        default="workspace/data/extract/entities.tsv",
        help="TSV file path for entity matches (default: %(default)s)",
    )
    parser.add_argument(
        "--files-list",
        help="Optional path to a text file listing HTML files to process (one per line). "
             "Paths may be absolute or relative to --input-root.",
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
        help="List the files that would be processed without writing outputs.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging output.",
    )
    return parser.parse_args(argv)


def load_file_list(path: Path) -> List[str]:
    entries: List[str] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
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
        candidates.extend(load_file_list(list_file))

    if explicit_files:
        candidates.extend(explicit_files)

    paths: List[Path] = []
    if candidates:
        for candidate in candidates:
            path = Path(candidate)
            if not path.is_absolute():
                path = input_root / path
            if path.suffix.lower() != ".html":
                logger.debug("Skipping non-HTML entry: %s", path)
                continue
            if path.exists():
                paths.append(path)
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


def ensure_parent(path: Path) -> None:
    if not path.parent.exists():
        path.parent.mkdir(parents=True, exist_ok=True)


def write_entities(
    entities_path: Path,
    doc_id: str,
    matches: Iterable[EntityMatch],
    header_written: bool,
) -> bool:
    ensure_parent(entities_path)

    write_header = not header_written and not entities_path.exists()

    with entities_path.open("a", encoding="utf-8") as handle:
        if write_header:
            handle.write("doc_id\ttype\tvalue\tstart\tend\tmatch\n")
            header_written = True

        for match in matches:
            handle.write(
                f"{doc_id}\t{match.entity_type}\t{match.value}\t"
                f"{match.start}\t{match.end}\t{match.matched}\n"
            )

    return header_written or write_header


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    input_root = Path(args.input_root).resolve()
    text_output_root = Path(args.output_text).resolve()
    main_output_root = Path(args.output_main_content).resolve()
    structured_output_root = Path(args.output_structured).resolve()
    entities_path = Path(args.entities_file).resolve()

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

    for output_dir in (text_output_root, main_output_root, structured_output_root):
        ensure_parent(output_dir)
        if not output_dir.exists():
            output_dir.mkdir(parents=True, exist_ok=True)

    text_extractor = HtmlTextExtractor()
    main_content_extractor = GithubMainContentExtractor(text_extractor)
    metadata_extractor = GithubRepoMetadataExtractor(text_extractor)
    entity_extractor = RegexEntityExtractor()
    header_written = entities_path.exists() and entities_path.stat().st_size > 0

    processed = 0
    skipped = 0

    for idx, html_path in enumerate(html_paths, start=1):
        doc_id = html_path.stem
        original_name = html_path.name
        text_output_path = text_output_root / f"text_{original_name}.txt"
        main_output_path = main_output_root / f"main_{original_name}.txt"
        structured_output_path = structured_output_root / f"structured_{original_name}.json"

        output_paths = [text_output_path, main_output_path, structured_output_path]
        if not args.force and all(path.exists() for path in output_paths):
            logger.debug(
                "Skipping existing outputs (use --force to override): %s",
                text_output_path,
            )
            skipped += 1
            continue

        html = html_path.read_text("utf-8", errors="ignore")
        text_result = text_extractor.extract(html)

        ensure_parent(text_output_path)
        text_output_path.write_text(text_result.text, encoding="utf-8")

        main_result = main_content_extractor.extract(html, title=text_result.title)
        ensure_parent(main_output_path)
        main_output_path.write_text(main_result.text, encoding="utf-8")

        metadata = metadata_extractor.extract(html, title=text_result.title)
        ensure_parent(structured_output_path)
        structured_output_path.write_text(
            json.dumps(metadata.to_dict(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        matches = entity_extractor.extract(html)
        header_written = write_entities(entities_path, doc_id, matches, header_written)

        processed += 1

        if processed % 25 == 0 or idx == len(html_paths):
            logger.info(
                "Processed %d/%d files (skipped %d)", processed, len(html_paths), skipped
            )

    logger.info("Extraction complete: %d processed, %d skipped", processed, skipped)
    logger.info("Text output: %s", text_output_root)
    logger.info("Main content output: %s", main_output_root)
    logger.info("Structured output: %s", structured_output_root)
    logger.info("Entities TSV: %s", entities_path)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
