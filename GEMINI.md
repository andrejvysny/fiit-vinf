# GEMINI.md: Project Overview and Commands

This document provides a comprehensive overview of the VINF data processing pipeline, designed to be used as a quick reference and instructional context for Gemini.

## 1. Project Overview

This project is a sophisticated data processing pipeline for analyzing GitHub repositories, enriching the data with information from Wikipedia, and providing a powerful search interface. The entire pipeline is orchestrated using Docker and a unified command-line interface (`bin/cli`).

**Core Technologies:**

*   **Language:** Python 3.9+
*   **Data Processing:** Apache Spark (via `pyspark`) for distributed processing of large datasets.
*   **Search/Indexing:** PyLucene for building and querying a full-text search index. A TF-IDF implementation is also included for comparison.
*   **Orchestration:** Docker and Docker Compose are used to manage and run the various services in a containerized environment.
*   **Dependencies:** Key Python libraries include `httpx` (for crawling), `pyyaml` (for configuration), and `pyspark`.

**Architecture & Pipeline Stages:**

The pipeline consists of several distinct stages, each implemented as a separate module and Docker service:

1.  **Crawl (`crawler/`):** A custom web crawler fetches HTML content from specified GitHub repositories based on rules in `config.yml`.
2.  **HTML Extraction (`spark/jobs/html_extractor.py`):** A Spark job processes the crawled HTML to extract plain text and entities (e.g., repository names, libraries).
3.  **Wikipedia Extraction (`spark/jobs/wiki_extractor.py`):** A Spark job parses a Wikipedia XML dump to create structured data files (TSVs) containing pages, categories, links, abstracts, etc.
4.  **Entity-Wiki Join (`spark/jobs/join_html_wiki.py`):** A Spark job enriches the extracted GitHub entities by joining them with the structured Wikipedia data.
5.  **Index Build (`lucene_indexer/build.py`):** A PyLucene process builds a full-text search index from the extracted text, entities, and joined wiki data.
6.  **Search (`lucene_indexer/search.py`):** A CLI allows for complex queries against the Lucene index, supporting simple, boolean, phrase, fuzzy, and range searches.

The main entry point for all operations is the `bin/cli` script, which simplifies running the various pipeline stages.

## 2. Building and Running the Project

All commands are executed via the `bin/cli` script, which wraps the underlying `docker compose` commands.

### Setup

```bash
# Install Python dependencies into a virtual environment
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Running the Full Pipeline

A single command can run the entire pipeline. The `--sample` and `--wiki-max-pages` flags are useful for testing.

```bash
# Run the complete pipeline in test mode
bin/cli pipeline --sample 100 --wiki-max-pages 1000

# Run the full production pipeline
bin/cli pipeline
```

### Running Individual Stages

Each stage of the pipeline can be run independently.

```bash
# 1. Extract from 100 sample HTML files
bin/cli extract --sample 100

# 2. Extract from 1000 sample Wikipedia pages
bin/cli wiki --wiki-max-pages 1000

# 3. Join the extracted data
bin/cli join

# 4. Build the Lucene search index
bin/cli lucene-build

# 5. Search the index
bin/cli lucene-search "python machine learning"
```

### Searching the Index

The search command supports several query types:

```bash
# Simple search
bin/cli lucene-search "python web"

# Boolean search
bin/cli lucene-search --query "python AND docker" --type boolean

# Phrase search
bin/cli lucene-search --query '"machine learning"' --type phrase

# Fuzzy search (typo tolerance)
bin/cli lucene-search --query "pyhton" --type fuzzy

# Range search on a specific field
bin/cli lucene-search --type range --field star_count --min-value 1000
```

### Direct Docker Commands

For more direct control or debugging, you can use `docker compose`:

```bash
# Run the Spark HTML extraction service
docker compose run --rm spark-extract

# Build the Lucene index
docker compose run --rm lucene-build

# Run a search query
docker compose run --rm lucene-search
```

## 3. Development Conventions

*   **Entry Point:** All pipeline operations are exposed through the master `bin/cli` script. This should be the primary interface for any interaction.
*   **Configuration:** The project is configured via `config.yml`. This file controls the crawler's behavior, file paths, and other operational parameters.
*   **Containerization:** All processing steps are designed to run within Docker containers, defined in `docker-compose.yml`. This ensures a consistent and reproducible environment. Spark jobs use the `apache/spark-py` image, and Lucene tasks use a `pylucene` image.
*   **Data Flow:** Data flows through a series of directories within the `workspace/` folder. Raw inputs are read, and the outputs of each stage are written to a corresponding subdirectory (e.g., `workspace/store/text`, `workspace/store/wiki`, `workspace/store/join`).
*   **Testing:** Unit tests are located in the `tests/` directory and can be run with `python -m unittest discover tests`.
*   **Memory Management:** For large-scale data processing, Spark memory settings can be configured via environment variables (e.g., `SPARK_DRIVER_MEMORY`, `SPARK_EXECUTOR_MEMORY`) as documented in the `README.md`.
