# Spark Pipeline - Run Instructions

This document provides run instructions for all three Spark-based jobs in the pipeline:
1. **HTML Extraction** - Extract entities and text from GitHub HTML pages
2. **Wikipedia Extraction** - Extract structured data from Wikipedia XML dumps
3. **Entity-Wikipedia Join** - Join HTML entities with Wikipedia canonical data

## Prerequisites

- Docker Desktop installed and running
- macOS (ARM64/Intel) or Linux
- Sufficient RAM based on task:
  - HTML extraction: 4-8GB
  - Wikipedia extraction (test): 4GB
  - Wikipedia extraction (full): 16-32GB
  - Entity join: 8-16GB
- Python 3.x with virtual environment

## Quick Start

### 1. HTML Extraction (GitHub Pages)

#### Sample Run (500 files)
```bash
# Run extraction on first 500 HTML files
bin/spark_extract --sample 500 --partitions 32 --force
```

Expected output:
- **Duration**: ~10 seconds
- **Memory**: 2GB driver, 1GB executor
- **Outputs**: `workspace/store/spark/text/` and `workspace/store/spark/entities/`

#### Full Dataset Run (~10GB, 28,353 files)
```bash
# Set higher memory for large dataset
export SPARK_DRIVER_MEMORY=6g
export SPARK_EXECUTOR_MEMORY=4g

# Run on all files with optimized partitioning
bin/spark_extract --partitions 256 --force
```

Recommended settings for ~10GB:
- **Partitions**: 256-512 (adjust based on file count)
- **Driver Memory**: 6-8GB
- **Executor Memory**: 4GB
- **Expected Duration**: 5-10 minutes

### 2. Wikipedia Extraction (100GB+ XML Dump)

#### Test Run (50-100 pages)
```bash
# Quick validation test
bin/spark_wiki_extract --wiki-max-pages 50 --partitions 8
```

Expected output:
- **Duration**: ~20 seconds
- **Memory**: 4GB driver, 2GB executor (auto-configured)
- **Outputs**: 6 TSV files in `workspace/store/wiki/`

#### Medium Test (1000 pages)
```bash
# Larger sample for testing
bin/spark_wiki_extract --wiki-max-pages 1000 --partitions 32
```

Expected output:
- **Duration**: ~3-5 minutes
- **Memory**: 4GB driver, 2GB executor
- **Pages processed**: 1000

#### Full Extraction (All ~7M Pages)

**For 32GB RAM System** (Recommended):
```bash
SPARK_DRIVER_MEMORY=12g SPARK_EXECUTOR_MEMORY=6g \
bin/spark_wiki_extract --partitions 512
```

**For 64GB+ RAM System** (High Performance):
```bash
SPARK_DRIVER_MEMORY=16g SPARK_EXECUTOR_MEMORY=8g \
bin/spark_wiki_extract --partitions 1024
```

Expected output:
- **Duration**: 2-3 hours (32GB system), 1.5-2 hours (64GB system)
- **Pages processed**: ~7 million
- **Throughput**: 650-1200 pages/sec
- **Outputs**: 6 TSV files totaling ~10-15GB

### 3. Entity-Wikipedia Join

#### Test Run (10K entities)
```bash
bin/spark_join_wiki \
  --entities workspace/store/entities/entities.tsv \
  --wiki workspace/store/wiki \
  --out workspace/store/join \
  --entities-max-rows 10000 \
  --partitions 32
```

Expected output:
- **Duration**: ~2-3 minutes
- **Memory**: 6GB driver, 3GB executor (auto-configured)

#### Full Join (All entities)
```bash
# For 16GB RAM system
bin/spark_join_wiki \
  --entities workspace/store/entities/entities.tsv \
  --wiki workspace/store/wiki \
  --out workspace/store/join

# For 32GB+ RAM system
SPARK_DRIVER_MEMORY=12g SPARK_EXECUTOR_MEMORY=6g \
bin/spark_join_wiki \
  --entities workspace/store/entities/entities.tsv \
  --wiki workspace/store/wiki \
  --out workspace/store/join \
  --partitions 128
```

Expected output:
- **Duration**: 5-10 minutes
- **Match rate**: 30-60% depending on entity types
- **Outputs**: 3 files in `workspace/store/join/`

## Command Options

### HTML Extraction (`bin/spark_extract`)
```bash
bin/spark_extract [OPTIONS]

Options:
  --sample N        Process only first N files
  --partitions N    Number of Spark partitions (default: 64)
  --force          Overwrite existing outputs
  --dry-run        List files without processing
  --local          Fallback to Python extractor (no Docker)
  --config PATH    Custom config file (default: config.yml)
```

### Wikipedia Extraction (`bin/spark_wiki_extract`)
```bash
bin/spark_wiki_extract [OPTIONS]

Options:
  --wiki-in DIR         Input directory with Wikipedia dumps (default: wiki_dump)
  --out DIR            Output directory for TSV files (default: workspace/store/wiki)
  --wiki-max-pages N   Limit pages for testing (default: all pages)
  --partitions N       Number of Spark partitions (default: 256)
  --log FILE           Log file path (default: logs/wiki_extract.jsonl)
  --dry-run           List files without processing

Environment variables:
  SPARK_DRIVER_MEMORY     Driver JVM memory (auto-set based on --wiki-max-pages)
  SPARK_EXECUTOR_MEMORY   Executor JVM memory (auto-set based on --wiki-max-pages)
```

### Entity-Wikipedia Join (`bin/spark_join_wiki`)
```bash
bin/spark_join_wiki [OPTIONS]

Options:
  --entities FILE       Path to entities.tsv (required)
  --wiki DIR           Directory with Wikipedia TSV files (required)
  --out DIR            Output directory (default: workspace/store/join)
  --entities-max-rows N Limit entity rows for testing
  --partitions N       Number of Spark partitions (default: 64)
  --log FILE           Log file path (default: logs/wiki_join.jsonl)
  --dry-run           Preview without writing outputs

Environment variables:
  SPARK_DRIVER_MEMORY     Driver JVM memory (default: 6g)
  SPARK_EXECUTOR_MEMORY   Executor JVM memory (default: 3g)
```

## Docker Compose Commands

### Build and Start Container

```bash
# Build the Spark container
docker compose -f docker-compose.spark.yml build

# Run with custom arguments
docker compose -f docker-compose.spark.yml run --rm spark \
  bash -c "pip install -q -r /opt/app/requirements.txt && \
           /opt/spark/bin/spark-submit --master local[*] \
           --driver-memory 4g /opt/app/spark/main.py --sample 1000"
```

### Interactive Shell

```bash
# Start interactive PySpark shell
docker compose -f docker-compose.spark.yml run --rm spark \
  /opt/spark/bin/pyspark --master local[*]
```

## Performance Tuning

### Memory Configuration

#### HTML Extraction
| Dataset Size | Files | Driver Memory | Executor Memory | Partitions |
|-------------|-------|---------------|-----------------|------------|
| Small       | <500  | 2GB           | 1GB             | 32         |
| Medium      | <5000 | 4GB           | 2GB             | 64-128     |
| Large       | <30000| 6-8GB         | 4GB             | 256-512    |

#### Wikipedia Extraction
| Mode | Pages | Driver Memory | Executor Memory | Partitions | Duration |
|------|-------|---------------|-----------------|------------|----------|
| Test | <1000 | 4GB | 2GB | 8-32 | <5 min |
| Full (16GB) | ~7M | 12GB | 6GB | 512 | 3-4 hours |
| Full (32GB) | ~7M | 12GB | 6GB | 512 | 2-3 hours |
| Full (64GB) | ~7M | 16GB | 8GB | 1024 | 1.5-2 hours |

**Critical**: Wikipedia extraction uses streaming architecture with NO caching. Memory is auto-configured by the wrapper script based on `--wiki-max-pages`.

#### Entity-Wikipedia Join
| System RAM | Driver Memory | Executor Memory | Partitions | Typical Duration |
|------------|---------------|-----------------|------------|------------------|
| 16GB       | 6GB           | 3GB             | 64         | 5-10 min |
| 32GB       | 12GB          | 6GB             | 128        | 3-5 min |

### Partition Guidelines

```bash
# Rule of thumb: 2-4 partitions per CPU core
# For 8-core machine: 16-32 partitions minimum
# For large datasets: ~100-200 files per partition

# Check optimal partitions
TOTAL_FILES=$(find workspace/store/html -name "*.html" | wc -l)
PARTITIONS=$((TOTAL_FILES / 100))  # ~100 files per partition
echo "Recommended partitions for $TOTAL_FILES files: $PARTITIONS"
```

## Monitoring

### View Spark UI (during execution)

```bash
# While job is running, Spark UI available at:
open http://localhost:4040
```

### Check Manifest

```bash
# View latest run manifest
ls -la runs/*/manifest.json
cat runs/$(ls -t runs/ | head -1)/manifest.json | jq .
```

### Monitor Resource Usage

```bash
# Watch Docker container stats
docker stats vinf-spark-extractor
```

## Troubleshooting

### Out of Memory Errors

#### HTML Extraction
```bash
# Increase driver memory
export SPARK_DRIVER_MEMORY=8g

# Increase partitions to reduce memory per partition
bin/spark_extract --partitions 512
```

#### Wikipedia Extraction
```bash
# For full extraction OOM errors:

# 1. Increase driver memory (most important)
SPARK_DRIVER_MEMORY=16g SPARK_EXECUTOR_MEMORY=8g \
bin/spark_wiki_extract --partitions 1024

# 2. Increase partitions for better parallelism
SPARK_DRIVER_MEMORY=12g SPARK_EXECUTOR_MEMORY=6g \
bin/spark_wiki_extract --partitions 1024

# 3. Enable system swap if needed (macOS/Linux)
# macOS: swap is automatic
# Linux: swapon -s (check status)

# 4. Close other applications to free memory
```

**Note**: The Wikipedia extractor uses streaming processing with buffer limits and NO caching to prevent OOM. If you still get OOM errors with recommended settings, your system may not have enough RAM for full extraction.

### Slow Performance

```bash
# Increase parallelism
bin/spark_extract --partitions 256

# Check CPU usage - should be near 100%
docker stats --no-stream
```

### Docker Issues

```bash
# Clean up containers and networks
docker compose -f docker-compose.spark.yml down
docker system prune -f

# Restart Docker Desktop if needed
```

## Fallback to Python Extractor

If Docker is unavailable:

```bash
# Use --local flag to run Python extractor
bin/spark_extract --local --limit 500
```

## Output Verification

### HTML Extraction
```bash
# Count processed files
ls workspace/store/spark/text/*.txt | wc -l

# Check entity counts
wc -l workspace/store/spark/entities/entities.tsv

# Compare with Python baseline (if available)
diff -u <(head workspace/store/python/entities/entities.tsv) \
        <(head workspace/store/spark/entities/entities.tsv)
```

### Wikipedia Extraction
```bash
# Verify all 6 TSV files created
ls -lh workspace/store/wiki/
# Expected: pages.tsv, categories.tsv, links.tsv, infobox.tsv, abstract.tsv, aliases.tsv

# Count total pages (excluding header)
tail -n +2 workspace/store/wiki/pages.tsv | wc -l
# Expected for full dump: ~7 million

# Sample page data
head -20 workspace/store/wiki/pages.tsv

# Check categories extracted
head -20 workspace/store/wiki/categories.tsv

# Check aliases for redirects
head -20 workspace/store/wiki/aliases.tsv

# Review manifest with statistics
cat runs/$(ls -t runs/ | head -1)/manifest.json | jq .
```

### Entity-Wikipedia Join
```bash
# Check all 3 output files created
ls -lh workspace/store/join/
# Expected: html_wiki.tsv, join_stats.json, html_wiki_agg.tsv

# View overall match statistics
cat workspace/store/join/join_stats.json | jq .

# Count matched vs unmatched entities
tail -n +2 workspace/store/join/html_wiki.tsv | \
  awk -F'\t' '{if ($5 != "") matched++; else unmatched++}
              END {print "Matched:", matched, "Unmatched:", unmatched}'

# View high-confidence matches
tail -n +2 workspace/store/join/html_wiki.tsv | \
  awk -F'\t' '$8 >= 0.8' | head -20

# Most common matched Wikipedia pages
tail -n +2 workspace/store/join/html_wiki.tsv | \
  awk -F'\t' '$6 != "" {print $6}' | \
  sort | uniq -c | sort -rn | head -20
```

## Complete Pipeline Workflow

To run the complete pipeline from scratch:

```bash
# Step 1: HTML Extraction (if not already done)
# Skip if you already have workspace/store/entities/entities.tsv
export SPARK_DRIVER_MEMORY=6g
export SPARK_EXECUTOR_MEMORY=4g
bin/spark_extract --partitions 256 --force

# Step 2: Wikipedia Extraction
# Test first with small sample
bin/spark_wiki_extract --wiki-max-pages 100 --partitions 8

# If test succeeds, run full extraction (requires 16-32GB RAM)
SPARK_DRIVER_MEMORY=12g SPARK_EXECUTOR_MEMORY=6g \
bin/spark_wiki_extract --partitions 512

# Step 3: Entity-Wikipedia Join
bin/spark_join_wiki \
  --entities workspace/store/entities/entities.tsv \
  --wiki workspace/store/wiki \
  --out workspace/store/join

# Step 4: Verify Results
cat workspace/store/join/join_stats.json | jq .
```

**Estimated Total Time**:
- HTML extraction: 5-10 minutes
- Wikipedia extraction: 2-3 hours
- Entity join: 5-10 minutes
- **Total**: ~2.5-3.5 hours

**Storage Requirements**:
- Input Wikipedia dump: ~104GB (uncompressed XML)
- Output Wikipedia TSVs: ~10-15GB
- HTML entities: ~1-5GB
- Join results: ~1-3GB
- **Total disk space needed**: ~150GB