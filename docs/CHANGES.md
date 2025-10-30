# Recent Changes to Entity Extractor

## 2025-10-30 - Major Updates

### 1. Unified Entrypoint via `__main__.py`

**Changed:**
- `python -m extractor` now runs the comprehensive entity extraction by default
- No arguments required - uses sensible defaults
- Old simple text extractor deprecated

**Usage:**
```bash
# Run with defaults (processes all HTML files)
python -m extractor

# Test with sample
python -m extractor --sample 10 --verbose
```

### 2. README Files in Dedicated Directory

**Changed:**
- README files now saved in `workspace/store/readme/` folder
- File naming: `{doc_id}.txt` (not `.readme.txt`)
- Completely separated from preprocessed text

**Before:**
```
workspace/store/text-preprocessed/
  ├── {doc_id}.txt
  └── {doc_id}.readme.txt  ❌ Mixed together
```

**After:**
```
workspace/store/
  ├── text-preprocessed/
  │   └── {doc_id}.txt      ✓ Clean text only
  └── readme/
      └── {doc_id}.txt      ✓ README files only
```

### 3. Raw Value Storage (No Normalization)

**Changed:**
- All entity values stored exactly as found in HTML/text
- No normalization or conversion applied

**Examples:**
- Stars: `"38,350"` (not `38350`)
- Forks: `"12.8k"` (not `12800`)
- Versions: `"v1.2.3"` (not `"1.2.3"`)
- Emails: as-is from source
- URLs: as-is from source
- Licenses: as-is from source

**Rationale:**
- Preserve original formatting
- Avoid data loss from normalization
- Allow downstream tools to handle conversion as needed

### 4. Removed Components

**Deleted:**
- ❌ `extractor/normalize.py` - No longer needed
- ❌ `extractor/extractor.py` - Deprecated (renamed to `.deprecated`)

**Kept:**
- ✓ `extractor/entity_main.py` - Main orchestrator
- ✓ `extractor/entity_extractors.py` - Entity extraction (now stores raw)
- ✓ `extractor/html_clean.py` - HTML preprocessing
- ✓ `extractor/regexes.py` - Compiled patterns
- ✓ `extractor/io_utils.py` - File I/O
- ✓ `extractor/__main__.py` - Unified entry point

## Migration Guide

### If you were using the old extractor:

**Old way:**
```bash
python -m extractor.extractor --input-root workspace/store/html
```

**New way:**
```bash
python -m extractor --in workspace/store/html
```

### If you relied on normalized values:

**Before (normalized):**
```tsv
doc_id    type          value
abc123    STAR_COUNT    38350     # Normalized
abc123    VERSION       1.2.3     # Leading 'v' removed
```

**After (raw):**
```tsv
doc_id    type          value
abc123    STAR_COUNT    38,350    # Raw from HTML
abc123    VERSION       v1.2.3    # Exactly as found
```

**To normalize in your code:**
```python
# Parse human-readable numbers
def parse_count(value: str) -> int:
    value = value.replace(',', '')
    if value.endswith('k'):
        return int(float(value[:-1]) * 1000)
    if value.endswith('M'):
        return int(float(value[:-1]) * 1000000)
    return int(value)

# Example
raw_stars = "38,350"
stars_int = parse_count(raw_stars)  # 38350
```

## Summary

✅ **Simpler CLI** - Just `python -m extractor`
✅ **Cleaner structure** - README files in dedicated folder
✅ **Raw values** - No data loss from normalization
✅ **Less code** - Removed unnecessary normalization module
✅ **All tests passing** - Smoke test validates everything works

## Documentation

- **Quick start:** `USAGE-EXTRACTOR.md`
- **Full docs:** `README-EXTRACTOR.md`
- **Run test:** `python tests/smoke_test.py`
