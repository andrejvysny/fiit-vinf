# Wikipedia-HTML Entity Join Plan

## Overview

This document specifies the join strategy for linking HTML entities (extracted from GitHub pages) with Wikipedia canonical knowledge. The goal is to enrich entities with authoritative Wikipedia information while maintaining high precision.

## Supported Entity Types

### Primary Targets (High Confidence)

| Entity Type | Wikipedia Target | Join Strategy |
|-------------|-----------------|---------------|
| LANG_STATS | Programming language pages | Normalized title match + category validation |
| LICENSE | Software license pages | Exact/canonical title match |
| TOPICS | Technology/software pages | Exploded topic-by-topic matching |

### Secondary Targets (Medium Confidence)

| Entity Type | Wikipedia Target | Join Strategy |
|-------------|-----------------|---------------|
| README | Keywords to relevant pages | Selective high-value terms only |
| URL | External link validation | Domain-level enrichment |

### Not Joined

| Entity Type | Reason |
|-------------|--------|
| STAR_COUNT | Numeric metric, no Wikipedia equivalent |
| FORK_COUNT | Numeric metric, no Wikipedia equivalent |
| EMAIL | Privacy concerns, no public mapping |

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

### Optimization Strategies

1. **Broadcast small dimension tables**
   ```python
   broadcast(canonical_df)  # If < 1GB
   ```

2. **Partition by join key**
   ```python
   repartition("norm_value")  # Before join
   ```

3. **Filter early**
   - Remove unsupported entity types before normalization
   - Skip entities with null/empty values

4. **Cache intermediate results**
   ```python
   canonical_df.cache()  # Reused multiple times
   ```

## Future Enhancements

1. **Fuzzy matching**: Levenshtein distance for typos
2. **ML-based disambiguation**: Learn from validated matches
3. **Multi-language support**: Cross-language Wikipedia links
4. **Temporal awareness**: Version-specific matches (e.g., "Python 2.7")
5. **Graph-based resolution**: Use Wikipedia link graph for context

## Usage Examples

### Basic Join
```bash
bin/spark_join_wiki \
  --entities workspace/store/entities/entities.tsv \
  --wiki workspace/store/wiki \
  --out workspace/store/join
```

### Development Mode
```bash
bin/spark_join_wiki \
  --entities workspace/store/entities/entities.tsv \
  --wiki workspace/store/wiki \
  --out workspace/store/join \
  --entities-max-rows 10000 \
  --partitions 32
```

## Validation Commands

```bash
# Check match rates
cat workspace/store/join/join_stats.json | jq .

# Sample high-confidence matches
grep -E "0\.[89]|1\.0" workspace/store/join/html_wiki.tsv | head -20

# Find unmatched popular entities
awk -F'\t' '$5==""' workspace/store/join/html_wiki.tsv | \
  cut -f3 | sort | uniq -c | sort -rn | head -20
```