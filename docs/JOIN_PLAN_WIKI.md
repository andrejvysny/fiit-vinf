# Wikipedia-HTML Entity Join Plan

## Overview

This document specifies the join strategy for linking HTML entities (extracted from GitHub pages) with Wikipedia canonical knowledge. The goal is to enrich entities with authoritative Wikipedia information while maintaining high precision.

## Supported Entity Types

The join pipeline supports the following entity types extracted from GitHub HTML pages. Entity types are defined in `spark/jobs/join_html_wiki.py:168-178`.

### Joinable Types (Matched with Wikipedia)

| Entity Type | Wikipedia Target | Join Strategy | Expected Match Rate |
|-------------|-----------------|---------------|---------------------|
| LANG_STATS | Programming language pages | Normalized title match + category validation | 60-90% |
| LICENSE | Software license pages | Exact/canonical title match | 80-95% |
| TOPICS | Technology/software pages | Exploded topic-by-topic matching | 30-50% |
| README | Keywords from README text | Selective high-value terms only | 10-30% |

### Metadata Types (Not Joined)

| Entity Type | Purpose | Join Support |
|-------------|---------|--------------|
| STAR_COUNT | Repository popularity metric | Used for filtering/ranking only |
| FORK_COUNT | Repository fork count | Used for filtering/ranking only |
| URL | External/internal links | Domain-level analysis (not entity resolution) |

**Note**: TOPICS entities are comma-separated and automatically exploded during joining, so `"python,django,postgresql"` becomes 3 separate join attempts.

## Normalization Rules

### Entity Value Normalization

```python
def normalize_entity_value(value, entity_type):
    # Special handling for TOPICS (comma-separated)
    if entity_type == 'TOPICS':
        topics = value.split(',')
        return [normalize_title(t.strip()) for t in topics]

    # Standard normalization
    return normalize_title(value)
```

### Title Normalization (Matching Wikipedia)

1. **Remove parenthetical suffixes**: Handle Wikipedia disambiguation
   - "MIT License" matches "MIT License (software)"
   - "Python" matches "Python (programming language)"

2. **Case folding**: Lowercase for matching
   - "Python" → "python"
   - "MIT" → "mit"

3. **ASCII folding**: Remove diacritics
   - "Réact" → "react"
   - "Zürich" → "zurich"

4. **Punctuation handling**: Collapse special characters
   - "C++" → "c"
   - "Node.js" → "node js"

5. **Whitespace normalization**: Single spaces
   - "Apache  License  2.0" → "apache license 2 0"

## Join Keys

### Primary Key: norm_title

```sql
entities.norm_value = canonical.norm_title
```

### Alias Resolution

```sql
-- First resolve aliases to canonical
entities.norm_value = aliases.alias_norm_title
  -> aliases.canonical_norm_title = pages.norm_title
```

### Multi-step Resolution

```mermaid
flowchart LR
    A[Entity Value<br/>"Python"] --> B[Normalize<br/>"python"]
    B --> C{Direct Match?}
    C -->|Yes| D[Page: Python<br/>programming language]
    C -->|No| E{Alias Match?}
    E -->|Yes| F[Resolve to<br/>Canonical]
    E -->|No| G[No Match]
```

## Collision Handling

### Disambiguation Strategy

When multiple Wikipedia pages match a normalized title:

1. **Exact case match** (highest priority)
   - Original entity value case == Wikipedia title case

2. **Namespace filtering**
   - Prefer namespace 0 (main articles)
   - Exclude meta namespaces

3. **Category hints**
   - Programming languages: has "Programming languages" category
   - Licenses: has "Software licenses" or "Free content licenses"
   - Technologies: has relevant technical categories

4. **Popularity signals** (if available)
   - Page with more incoming links
   - Page with longer content

### Example Disambiguation

Entity: "Python" (from TOPICS)

Candidates:
1. Python (programming language) - ns:0, category:"Programming languages" ✓
2. Python (genus) - ns:0, category:"Snakes"
3. Monty Python - ns:0, category:"Comedy"

Result: Match #1 with confidence 0.9

## Confidence Scoring

### Score Calculation

```python
confidence = base_confidence + bonuses

Base scores:
- Direct match: 0.6
- Alias match: 0.5
- Heuristic match: 0.4

Bonuses (cumulative):
- Exact case match: +0.2
- Category validation: +0.1
- Single unambiguous result: +0.1
```

### Confidence Levels

| Score | Interpretation | Action |
|-------|---------------|--------|
| 0.9-1.0 | High confidence | Auto-accept |
| 0.7-0.9 | Good match | Accept with review |
| 0.5-0.7 | Possible match | Manual validation needed |
| < 0.5 | Low confidence | Likely false positive |

## Quality Assurance

### Pre-join Validation

1. **Entity filtering**
   - Remove generic terms ("test", "example", "todo")
   - Skip single-character values
   - Validate entity format (e.g., URLs are valid)

2. **Wikipedia data quality**
   - Skip stub articles (< 100 chars abstract)
   - Require infobox for technical pages
   - Validate redirect chains

### Post-join Validation

1. **Statistical checks**
   - Match rate by entity type (expected: 30-60%)
   - Unique Wikipedia pages (should be << total entities)
   - Confidence distribution (most > 0.7)

2. **Sample inspection**
   - Manual review of 100 random joins
   - Check high-confidence mismatches
   - Validate disambiguation choices

## Edge Cases

### Handled Scenarios

1. **Multiple topics in single entity**
   ```
   TOPICS: "python,django,postgresql"
   -> Explode and join each separately
   ```

2. **Version numbers**
   ```
   "Python 3.11" -> normalize to "python" -> match "Python (programming language)"
   ```

3. **Acronyms**
   ```
   "MIT" -> could be "MIT License" or "Massachusetts Institute of Technology"
   -> Use entity_type context to disambiguate
   ```

4. **Redirect chains**
   ```
   "MIT License" -> "MIT License (software)" -> "MIT License"
   -> Follow chain to canonical
   ```

### Unhandled Scenarios

1. **Composite entities**: "React/Redux" - treated as single term
2. **Misspellings**: "Pythn" won't match "Python"
3. **Translations**: "パイソン" won't match "Python"
4. **Temporal changes**: Historical renames not tracked

## Metrics & Reporting

### Join Statistics (output to `join_stats.json`)

```json
{
  "total_entities": 150000,
  "matched_entities": 45000,
  "match_rate": 30.0,
  "unique_wiki_pages": 1200,
  "by_type": {
    "TOPICS": {
      "total": 100000,
      "matched": 35000,
      "rate": 35.0
    },
    "LICENSE": {
      "total": 5000,
      "matched": 4500,
      "rate": 90.0
    }
  }
}
```

### Per-document Aggregates (`html_wiki_agg.tsv`)

| doc_id | entity_type | total | matched |
|--------|------------|-------|---------|
| abc123 | TOPICS | 15 | 12 |
| abc123 | LICENSE | 1 | 1 |

## Performance Considerations

### Optimization Strategies (Already Implemented)

1. **Canonical mapping cached**
   - `canonical_df.cache()` (line 418) - Reused for all entity joins
   - Contains both direct pages and alias mappings
   - Typically < 500MB for English Wikipedia

2. **Topic explosion handled efficiently**
   - TOPICS values are split and exploded using Spark's `F.explode()`
   - Each topic joins independently
   - No manual iteration or collecting to driver

3. **Early filtering**
   - Unsupported entity types filtered before normalization (line 241-243)
   - Namespace filtering applied during canonical mapping (line 131)
   - Null/empty values handled via DataFrame operations

4. **Adaptive query execution**
   - Spark AQE enabled by default in wrapper script
   - Automatically optimizes shuffle partitions
   - Coalesces partitions for small result sets

### Memory Configuration

**Default** (16GB RAM system):
```bash
SPARK_DRIVER_MEMORY=6g
SPARK_EXECUTOR_MEMORY=3g
--partitions 64
```

**Recommended** (32GB RAM system):
```bash
SPARK_DRIVER_MEMORY=12g
SPARK_EXECUTOR_MEMORY=6g
--partitions 128
```

**Performance Notes**:
- Join is significantly faster than extraction (typically < 10 minutes)
- Most time spent on initial data loading
- Canonical mapping cache improves multi-type joins
- Statistics calculation adds ~20% overhead

## Future Enhancements

1. **Fuzzy matching**: Levenshtein distance for typos
2. **ML-based disambiguation**: Learn from validated matches
3. **Multi-language support**: Cross-language Wikipedia links
4. **Temporal awareness**: Version-specific matches (e.g., "Python 2.7")
5. **Graph-based resolution**: Use Wikipedia link graph for context

## Usage Examples

### Prerequisites

Before running the join, ensure both inputs exist:
```bash
# 1. Wikipedia extraction must be completed
ls -lh workspace/store/wiki/*.tsv
# Expected: pages.tsv, aliases.tsv, categories.tsv, etc.

# 2. HTML entity extraction must be completed
ls -lh workspace/store/entities/entities.tsv
# or entities2.tsv depending on your extraction
```

### Basic Join (Production)
```bash
bin/spark_join_wiki \
  --entities workspace/store/entities/entities.tsv \
  --wiki workspace/store/wiki \
  --out workspace/store/join
```

### Development Mode (Small Sample)
```bash
# Test with first 10,000 entities
bin/spark_join_wiki \
  --entities workspace/store/entities/entities.tsv \
  --wiki workspace/store/wiki \
  --out workspace/store/join \
  --entities-max-rows 10000 \
  --partitions 32
```

### High-Memory System
```bash
# For systems with 32GB+ RAM
SPARK_DRIVER_MEMORY=12g SPARK_EXECUTOR_MEMORY=6g \
bin/spark_join_wiki \
  --entities workspace/store/entities/entities.tsv \
  --wiki workspace/store/wiki \
  --out workspace/store/join \
  --partitions 128
```

## Output Files

The join pipeline produces three output files in the specified output directory:

### 1. `html_wiki.tsv`
Main join results with one row per entity-Wikipedia match attempt.

**Schema**:
| Column | Type | Description |
|--------|------|-------------|
| doc_id | string | HTML document identifier (SHA256) |
| entity_type | string | Type of entity (TOPICS, LICENSE, etc.) |
| entity_value | string | Original entity value from HTML |
| norm_value | string | Normalized value used for matching |
| wiki_page_id | long? | Wikipedia page ID (null if no match) |
| wiki_title | string? | Canonical Wikipedia title |
| join_key | string? | Normalized key that matched |
| confidence | float? | Match confidence score (0.0-1.0) |

### 2. `join_stats.json`
Overall join statistics and match rates by entity type.

**Example**:
```json
{
  "total_entities": 150000,
  "matched_entities": 45000,
  "match_rate": 30.0,
  "unique_wiki_pages": 1200,
  "by_type": {
    "TOPICS": {
      "total": 100000,
      "matched": 35000,
      "rate": 35.0
    },
    "LICENSE": {
      "total": 5000,
      "matched": 4500,
      "rate": 90.0
    }
  }
}
```

### 3. `html_wiki_agg.tsv`
Per-document aggregate statistics.

**Schema**:
| Column | Type | Description |
|--------|------|-------------|
| doc_id | string | HTML document identifier |
| entity_type | string | Entity type |
| total | long | Total entities of this type |
| matched | long | Number that matched Wikipedia |

## Validation Commands

### Check Overall Match Rates
```bash
# View full statistics
cat workspace/store/join/join_stats.json | jq .

# Quick match rate summary
cat workspace/store/join/join_stats.json | jq '.match_rate'

# Match rates by type
cat workspace/store/join/join_stats.json | jq '.by_type'
```

### Inspect Join Results
```bash
# Count total rows
wc -l workspace/store/join/html_wiki.tsv

# Count matched vs unmatched
tail -n +2 workspace/store/join/html_wiki.tsv | \
  awk -F'\t' '{if ($5 != "") matched++; else unmatched++}
              END {print "Matched:", matched, "Unmatched:", unmatched}'

# Sample high-confidence matches (confidence >= 0.8)
tail -n +2 workspace/store/join/html_wiki.tsv | \
  awk -F'\t' '$8 >= 0.8' | head -20

# Sample matched TOPICS
grep -P '^[^\t]+\tTOPICS\t' workspace/store/join/html_wiki.tsv | \
  grep -v $'\t\t' | head -20
```

### Find Common Unmatched Entities
```bash
# Most common unmatched entity values
tail -n +2 workspace/store/join/html_wiki.tsv | \
  awk -F'\t' '$5 == "" {print $3}' | \
  sort | uniq -c | sort -rn | head -20

# Unmatched by type
tail -n +2 workspace/store/join/html_wiki.tsv | \
  awk -F'\t' '$5 == "" {print $2}' | \
  sort | uniq -c
```

### Verify Output Quality
```bash
# Check for duplicate joins (same entity matched multiple times)
tail -n +2 workspace/store/join/html_wiki.tsv | \
  awk -F'\t' '$5 != "" {print $1"\t"$3}' | \
  sort | uniq -c | sort -rn | head -20

# Most frequently matched Wikipedia pages
tail -n +2 workspace/store/join/html_wiki.tsv | \
  awk -F'\t' '$6 != "" {print $6}' | \
  sort | uniq -c | sort -rn | head -20
```