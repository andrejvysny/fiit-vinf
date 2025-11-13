# Spark HTML Extractor - Run Instructions

## Prerequisites

- Docker Desktop installed and running
- macOS (ARM64/Intel) or Linux
- At least 4GB RAM available for Docker
- Python 3.x with virtual environment

## Quick Start

### Sample Run (500 files)

```bash
# Run extraction on first 500 HTML files
bin/spark_extract --sample 500 --partitions 32 --force
```

Expected output:
- **Duration**: ~10 seconds
- **Memory**: 2GB driver, 1GB executor
- **Outputs**: `workspace/store/spark/text/` and `workspace/store/spark/entities/`

### Full Dataset Run (~10GB, 28,353 files)

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

## Command Options

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

| Dataset Size | Files | Driver Memory | Executor Memory | Partitions |
|-------------|-------|---------------|-----------------|------------|
| Small       | <500  | 2GB           | 1GB             | 32         |
| Medium      | <5000 | 4GB           | 2GB             | 64-128     |
| Large       | <30000| 6-8GB         | 4GB             | 256-512    |

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

```bash
# Increase driver memory
export SPARK_DRIVER_MEMORY=8g

# Increase partitions to reduce memory per partition
bin/spark_extract --partitions 512
```

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

```bash
# Count processed files
ls workspace/store/spark/text/*.txt | wc -l

# Check entity counts
wc -l workspace/store/spark/entities/entities.tsv

# Compare with Python baseline (if available)
diff -u <(head workspace/store/python/entities/entities.tsv) \
        <(head workspace/store/spark/entities/entities.tsv)
```