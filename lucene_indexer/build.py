"""
PyLucene Index Builder.

Builds a Lucene index from:
1. Extracted text documents (workspace/store/text/)
2. Entity annotations (workspace/store/entities/entities.tsv)
3. Wiki join results (workspace/store/join/html_wiki.tsv)

Usage:
    python -m lucene_indexer.build --config config.yml
    python -m lucene_indexer.build --text-dir workspace/store/text --output workspace/store/lucene_index
"""

import argparse
import csv
import json
import logging
import os
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

# Try to import PyLucene - provide helpful error if not available
try:
    import lucene
    from java.nio.file import Paths as JPaths
    from org.apache.lucene.analysis.standard import StandardAnalyzer
    from org.apache.lucene.document import (
        Document, Field, StringField, TextField,
        IntPoint, LongPoint, StoredField, NumericDocValuesField
    )
    from org.apache.lucene.index import IndexWriter, IndexWriterConfig
    from org.apache.lucene.store import FSDirectory
    LUCENE_AVAILABLE = True
except ImportError:
    LUCENE_AVAILABLE = False

from .schema import IndexSchema, FieldType, FIELD_DEFINITIONS

# Import stats module if available
try:
    from spark.lib.stats import PipelineStats, save_pipeline_summary
    STATS_AVAILABLE = True
except ImportError:
    STATS_AVAILABLE = False

logger = logging.getLogger(__name__)


class LuceneIndexBuilder:
    """
    Builds a Lucene index from extracted documents with entity and wiki enrichment.
    """

    def __init__(
        self,
        text_dir: Path,
        output_dir: Path,
        entities_path: Optional[Path] = None,
        wiki_join_path: Optional[Path] = None,
    ):
        """
        Initialize the index builder.

        Args:
            text_dir: Directory containing text documents (.txt files)
            output_dir: Directory where Lucene index will be created
            entities_path: Path to entities.tsv (optional)
            wiki_join_path: Path to html_wiki.tsv join results (optional)
        """
        self.text_dir = Path(text_dir)
        self.output_dir = Path(output_dir)
        self.entities_path = Path(entities_path) if entities_path else None
        self.wiki_join_path = Path(wiki_join_path) if wiki_join_path else None

        self.schema = IndexSchema()
        self._entities_cache: Dict[str, Dict] = {}
        self._wiki_cache: Dict[str, Dict] = {}

        # Stats
        self.stats = {
            "docs_indexed": 0,
            "docs_with_entities": 0,
            "docs_with_wiki": 0,
            "total_entities": 0,
            "total_wiki_matches": 0,
            "start_time": None,
            "end_time": None,
        }

    def _init_lucene(self):
        """Initialize JVM for PyLucene."""
        if not LUCENE_AVAILABLE:
            raise RuntimeError(
                "PyLucene is not installed. Install it via:\n"
                "  pip install lucene  # May require system-level Java JDK\n"
                "  or build from source: https://lucene.apache.org/pylucene/\n\n"
                "For fallback, use the standard TF-IDF indexer:\n"
                "  python -m indexer.build --config config.yml"
            )

        # Initialize JVM if not already started
        if not lucene.getVMEnv():
            lucene.initVM(vmargs=['-Djava.awt.headless=true'])
        else:
            lucene.getVMEnv().attachCurrentThread()

    def _load_entities(self):
        """Load entity annotations into cache keyed by doc_id."""
        if not self.entities_path or not self.entities_path.exists():
            logger.info("No entities file provided or found")
            return

        logger.info(f"Loading entities from {self.entities_path}")
        with open(self.entities_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                doc_id = row.get('doc_id', '')
                entity_type = row.get('type', '')
                value = row.get('value', '')

                if not doc_id:
                    continue

                if doc_id not in self._entities_cache:
                    self._entities_cache[doc_id] = {
                        'topics': [],
                        'languages': [],
                        'license': None,
                        'star_count': None,
                        'fork_count': None,
                        'url': None,
                    }

                cache = self._entities_cache[doc_id]

                if entity_type == 'TOPICS':
                    # Topics may be comma-separated
                    topics = [t.strip() for t in value.split(',') if t.strip()]
                    cache['topics'].extend(topics)
                elif entity_type == 'LANG_STATS':
                    # Languages may be comma-separated
                    langs = [l.strip() for l in value.split(',') if l.strip()]
                    cache['languages'].extend(langs)
                elif entity_type == 'LICENSE':
                    cache['license'] = value
                elif entity_type == 'STAR_COUNT':
                    try:
                        cache['star_count'] = int(value.replace(',', ''))
                    except (ValueError, AttributeError):
                        pass
                elif entity_type == 'FORK_COUNT':
                    try:
                        cache['fork_count'] = int(value.replace(',', ''))
                    except (ValueError, AttributeError):
                        pass
                elif entity_type == 'URL':
                    cache['url'] = value

                self.stats['total_entities'] += 1

        logger.info(f"Loaded entities for {len(self._entities_cache)} documents")

    def _load_wiki_joins(self):
        """Load wiki join results into cache keyed by doc_id."""
        if not self.wiki_join_path or not self.wiki_join_path.exists():
            logger.info("No wiki join file provided or found")
            return

        logger.info(f"Loading wiki joins from {self.wiki_join_path}")
        with open(self.wiki_join_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                doc_id = row.get('doc_id', '')
                wiki_page_id = row.get('wiki_page_id', '')
                wiki_title = row.get('wiki_title', '')
                wiki_abstract = row.get('wiki_abstract', '')
                wiki_categories = row.get('wiki_categories', '')
                confidence = row.get('confidence', '')

                if not doc_id or not wiki_page_id:
                    continue

                if doc_id not in self._wiki_cache:
                    self._wiki_cache[doc_id] = {
                        'wiki_page_ids': [],
                        'wiki_titles': [],
                        'wiki_categories': [],
                        'wiki_abstracts': [],
                        'join_confidences': [],
                    }

                cache = self._wiki_cache[doc_id]
                try:
                    cache['wiki_page_ids'].append(int(wiki_page_id))
                except (ValueError, TypeError):
                    pass
                if wiki_title:
                    cache['wiki_titles'].append(wiki_title)
                if wiki_abstract:
                    cache['wiki_abstracts'].append(wiki_abstract)
                if wiki_categories:
                    # wiki_categories is pipe-separated
                    for cat in wiki_categories.split('|'):
                        cat = cat.strip()
                        if cat:
                            cache['wiki_categories'].append(cat)
                if confidence:
                    cache['join_confidences'].append(confidence)

                self.stats['total_wiki_matches'] += 1

        logger.info(f"Loaded wiki joins for {len(self._wiki_cache)} documents")

    def _create_lucene_document(
        self,
        doc_id: str,
        title: str,
        content: str,
        path: str,
    ) -> 'Document':
        """
        Create a Lucene Document with all fields populated.
        """
        doc = Document()

        # Core fields
        doc.add(StoredField("doc_id", doc_id))  # StoredField only (not indexed)
        doc.add(TextField("title", title, Field.Store.YES))
        doc.add(TextField("content", content, Field.Store.NO))  # Large, don't store
        doc.add(StoredField("path", path))

        # Content length for range queries
        content_length = len(content)
        doc.add(IntPoint("content_length", content_length))
        doc.add(StoredField("content_length", content_length))

        # Indexed timestamp
        indexed_at = int(time.time() * 1000)
        doc.add(LongPoint("indexed_at", indexed_at))
        doc.add(StoredField("indexed_at", indexed_at))

        # Entity fields
        if doc_id in self._entities_cache:
            entities = self._entities_cache[doc_id]
            self.stats['docs_with_entities'] += 1

            # Topics (multi-valued text field)
            for topic in set(entities.get('topics', [])):
                doc.add(TextField("topics", topic, Field.Store.YES))

            # Languages (multi-valued text field)
            for lang in set(entities.get('languages', [])):
                doc.add(TextField("languages", lang, Field.Store.YES))

            # License (string field for exact match)
            if entities.get('license'):
                doc.add(StringField("license", entities['license'], Field.Store.YES))

            # Star count (IntPoint for range + stored for retrieval)
            if entities.get('star_count') is not None:
                star_count = entities['star_count']
                doc.add(IntPoint("star_count", star_count))
                doc.add(StoredField("star_count", star_count))
                doc.add(NumericDocValuesField("star_count_sort", star_count))

            # Fork count (IntPoint for range + stored for retrieval)
            if entities.get('fork_count') is not None:
                fork_count = entities['fork_count']
                doc.add(IntPoint("fork_count", fork_count))
                doc.add(StoredField("fork_count", fork_count))

            # URL (StoredField only - not indexed)
            if entities.get('url'):
                doc.add(StoredField("url", entities['url']))

        # Wiki enrichment fields
        if doc_id in self._wiki_cache:
            wiki = self._wiki_cache[doc_id]
            self.stats['docs_with_wiki'] += 1

            # Wiki page IDs (multi-valued long point)
            for page_id in set(wiki.get('wiki_page_ids', [])):
                doc.add(LongPoint("wiki_page_id", page_id))
                doc.add(StoredField("wiki_page_id", page_id))

            # Wiki titles (multi-valued text field)
            for wiki_title in set(wiki.get('wiki_titles', [])):
                doc.add(TextField("wiki_title", wiki_title, Field.Store.YES))

            # Wiki categories (multi-valued text field)
            for category in set(wiki.get('wiki_categories', [])):
                doc.add(TextField("wiki_categories", category, Field.Store.YES))

            # Wiki abstracts (single text field, concatenated)
            abstracts = wiki.get('wiki_abstracts', [])
            if abstracts:
                combined_abstract = ' '.join(abstracts[:3])  # Limit to 3
                doc.add(TextField("wiki_abstract", combined_abstract, Field.Store.YES))

            # Join confidences
            for conf in set(wiki.get('join_confidences', [])):
                doc.add(StringField("join_confidence", conf, Field.Store.YES))

        return doc

    def build(self, limit: Optional[int] = None, dry_run: bool = False) -> Dict:
        """
        Build the Lucene index.

        Args:
            limit: Maximum number of documents to index (for testing)
            dry_run: If True, calculate stats without writing index

        Returns:
            Dictionary with build statistics
        """
        self.stats['start_time'] = datetime.now().isoformat()

        # Load enrichment data
        self._load_entities()
        self._load_wiki_joins()

        if dry_run:
            logger.info("Dry run mode - counting documents only")
            count = 0
            for txt_file in self.text_dir.glob("**/*.txt"):
                count += 1
                if limit and count >= limit:
                    break
            self.stats['docs_indexed'] = count
            self.stats['end_time'] = datetime.now().isoformat()
            return self.stats

        # Initialize Lucene
        self._init_lucene()

        # Create index directory
        self.output_dir.mkdir(parents=True, exist_ok=True)
        index_path = JPaths.get(str(self.output_dir))
        directory = FSDirectory.open(index_path)

        # Configure analyzer and writer
        analyzer = StandardAnalyzer()
        config = IndexWriterConfig(analyzer)
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE)

        writer = IndexWriter(directory, config)

        try:
            # Index documents
            txt_files = list(self.text_dir.glob("**/*.txt"))
            total_files = len(txt_files)
            logger.info(f"Found {total_files} text files to index")

            for i, txt_file in enumerate(txt_files):
                if limit and i >= limit:
                    break

                try:
                    # Read document content
                    content = txt_file.read_text(encoding='utf-8', errors='ignore')

                    # Document ID from filename (without extension)
                    doc_id = txt_file.stem

                    # Title: first line or filename
                    lines = content.split('\n')
                    title = lines[0][:200] if lines else txt_file.name

                    # Create and add Lucene document
                    lucene_doc = self._create_lucene_document(
                        doc_id=doc_id,
                        title=title,
                        content=content,
                        path=str(txt_file.absolute()),
                    )
                    writer.addDocument(lucene_doc)
                    self.stats['docs_indexed'] += 1

                    # Progress logging
                    if (i + 1) % 100 == 0:
                        logger.info(f"Indexed {i + 1}/{total_files} documents...")

                except Exception as e:
                    logger.warning(f"Failed to index {txt_file}: {e}")

            # Commit and close
            writer.commit()
            logger.info(f"Index committed with {self.stats['docs_indexed']} documents")

        finally:
            writer.close()
            directory.close()

        self.stats['end_time'] = datetime.now().isoformat()

        # Calculate duration
        start_dt = datetime.fromisoformat(self.stats['start_time'])
        end_dt = datetime.fromisoformat(self.stats['end_time'])
        self.stats['duration_seconds'] = round((end_dt - start_dt).total_seconds(), 2)

        # Write manifest
        self._write_manifest()

        # Save pipeline stats if available
        self._save_pipeline_stats()

        return self.stats

    def _save_pipeline_stats(self):
        """Save stats to the unified stats directory."""
        if not STATS_AVAILABLE:
            logger.info("Stats module not available, skipping pipeline stats")
            return

        try:
            pipeline_stats = PipelineStats("lucene_index")
            pipeline_stats.set_config(
                text_dir=str(self.text_dir),
                output_dir=str(self.output_dir),
                entities_path=str(self.entities_path) if self.entities_path else None,
                wiki_join_path=str(self.wiki_join_path) if self.wiki_join_path else None,
            )
            pipeline_stats.set_inputs(
                text_dir=str(self.text_dir),
                entities_file=str(self.entities_path) if self.entities_path else None,
                wiki_join_file=str(self.wiki_join_path) if self.wiki_join_path else None,
                total_items=self.stats['docs_indexed'],
            )
            pipeline_stats.set_outputs(
                index_dir=str(self.output_dir),
                docs_indexed=self.stats['docs_indexed'],
                docs_with_entities=self.stats['docs_with_entities'],
                docs_with_wiki=self.stats['docs_with_wiki'],
            )
            pipeline_stats.set_nested("index_stats", "total_entities", self.stats['total_entities'])
            pipeline_stats.set_nested("index_stats", "total_wiki_matches", self.stats['total_wiki_matches'])
            pipeline_stats.set_nested("performance", "docs_per_second",
                round(self.stats['docs_indexed'] / max(self.stats.get('duration_seconds', 1), 0.01), 2))
            pipeline_stats.finalize("completed")
            stats_path = pipeline_stats.save()
            logger.info(f"Stats saved to: {stats_path}")

            # Also regenerate pipeline summary
            save_pipeline_summary()
        except Exception as e:
            logger.warning(f"Failed to save pipeline stats: {e}")

    def _write_manifest(self):
        """Write index manifest with metadata."""
        manifest = {
            "index_type": "lucene",
            "created_at": self.stats['end_time'],
            "schema_version": "1.0",
            "fields": list(FIELD_DEFINITIONS.keys()),
            "stats": self.stats,
            "config": {
                "text_dir": str(self.text_dir),
                "entities_path": str(self.entities_path) if self.entities_path else None,
                "wiki_join_path": str(self.wiki_join_path) if self.wiki_join_path else None,
            }
        }

        manifest_path = self.output_dir / "manifest.json"
        with open(manifest_path, 'w', encoding='utf-8') as f:
            json.dump(manifest, f, indent=2)

        logger.info(f"Wrote manifest to {manifest_path}")


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Build PyLucene index from extracted documents"
    )
    parser.add_argument(
        '--config',
        default='config.yml',
        help='Path to configuration file'
    )
    parser.add_argument(
        '--text-dir',
        default=None,
        help='Directory containing text documents (overrides config)'
    )
    parser.add_argument(
        '--entities',
        default=None,
        help='Path to entities.tsv (overrides config)'
    )
    parser.add_argument(
        '--wiki-join',
        default=None,
        help='Path to wiki join TSV (overrides config)'
    )
    parser.add_argument(
        '--output',
        default=None,
        help='Output directory for Lucene index'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='Maximum documents to index (for testing)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Calculate stats without writing index'
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    """Main entry point."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s'
    )

    args = parse_args(argv)

    # Load config if available
    try:
        sys.path.insert(0, str(Path(__file__).parent.parent))
        from config_loader import load_yaml_config
        config = load_yaml_config(args.config)
        workspace = config.get('workspace', 'workspace')
    except Exception:
        workspace = 'workspace'

    # Resolve paths
    text_dir = Path(args.text_dir or f"{workspace}/store/text")
    entities_path = Path(args.entities or f"{workspace}/store/entities/entities.tsv")
    wiki_join_path = Path(args.wiki_join or f"{workspace}/store/join/html_wiki.tsv")
    output_dir = Path(args.output or f"{workspace}/store/lucene_index")

    if not text_dir.exists():
        logger.error(f"Text directory not found: {text_dir}")
        return 1

    # Build index
    builder = LuceneIndexBuilder(
        text_dir=text_dir,
        output_dir=output_dir,
        entities_path=entities_path if entities_path.exists() else None,
        wiki_join_path=wiki_join_path if wiki_join_path.exists() else None,
    )

    try:
        stats = builder.build(limit=args.limit, dry_run=args.dry_run)

        print("\n" + "=" * 60)
        print("LUCENE INDEX BUILD COMPLETE")
        print("=" * 60)
        print(f"Documents indexed: {stats['docs_indexed']}")
        print(f"Documents with entities: {stats['docs_with_entities']}")
        print(f"Documents with wiki matches: {stats['docs_with_wiki']}")
        print(f"Total entities: {stats['total_entities']}")
        print(f"Total wiki matches: {stats['total_wiki_matches']}")
        print(f"Output directory: {output_dir}")
        print("=" * 60)

        return 0

    except RuntimeError as e:
        logger.error(str(e))
        return 1


if __name__ == "__main__":
    sys.exit(main())
