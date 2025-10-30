# Entity Extractor - Summary of Latest Updates

## New Features Added (2025-10-30)

### 1. Raw Text Extraction

**What**: Extract text with only HTML tags removed (no boilerplate removal)

**Where**: `workspace/store/text/`

**Why**: Preserve all content including CSS, JavaScript, navigation for comprehensive analysis

**Example**:
```bash
python -m extractor --sample 10
```

**Difference**:
- **Raw text** (443 lines): Contains CSS, JSON, navigation, all HTML content
- **Preprocessed** (305 lines): Clean text only, boilerplate removed

### 2. Force Flag (--force)

**What**: Overwrite existing files instead of skipping them

**Why**: Regenerate outputs without manual cleanup

**Example**:
```bash
# Skip existing files (default)
python -m extractor

# Force reprocess everything
python -m extractor --force

# Force reprocess sample
python -m extractor --sample 100 --force
```

**Behavior**:
- **Without --force**: Files are skipped if they exist
- **With --force**: All files are reprocessed and overwritten

### 3. Smart Skip Behavior

**What**: Automatically skip files that already exist

**Why**: Save time on incremental processing

**How it works**:
- Checks if both raw text AND preprocessed text exist
- If both exist: skip processing (unless --force)
- If either missing: process the file
- Entities are always regenerated (cheap operation)

**Output example**:
```
Files processed:      0
Files skipped:        150
Raw text written:     0
Preprocessed written: 0
```

## Complete Output Structure

```
workspace/store/
├── entities/
│   └── entities.tsv              # Entities with offsets
├── text/                          # ← NEW: Raw text
│   └── {doc_id}.txt              #    HTML tags removed only
├── text-preprocessed/             # Clean text
│   └── {doc_id}.txt              #    Boilerplate removed
└── readme/                        # README files only
    └── {doc_id}.txt
```

## CLI Options Summary

| Flag | Default | Description |
|------|---------|-------------|
| `--in` | `workspace/store/html` | Input HTML directory |
| `--entities-out` | `workspace/store/entities/entities.tsv` | Entities TSV |
| `--text-out` | `workspace/store/text` | **Raw text** output |
| `--preproc-out` | `workspace/store/text-preprocessed` | Preprocessed text |
| `--readme-out` | `workspace/store/readme` | README files |
| `--sample N` | (none) | Process N files |
| `--force` | false | **Overwrite existing files** |
| `--dry-run` | false | List files only |
| `--verbose` | false | Debug logging |

## Usage Examples

### Basic Usage
```bash
# Run with defaults (skip existing)
python -m extractor

# Test with 10 files
python -m extractor --sample 10 --verbose
```

### Force Reprocess
```bash
# Regenerate all outputs
python -m extractor --force

# Regenerate sample
python -m extractor --sample 100 --force
```

### Incremental Processing
```bash
# First run: process all files
python -m extractor

# Second run: skip already processed files
python -m extractor
# Output: Files skipped: 28353

# Add new HTML files...
# Third run: process only new files
python -m extractor
# Output: Files processed: 50, Files skipped: 28353
```

## Performance

### First Run (No Files Exist)
- All files processed
- All outputs written
- ~25 files/second

### Subsequent Runs (Files Exist)
- Files skipped instantly
- ~1000 files/second (skip check only)
- Use `--force` to reprocess

### With --force
- All files reprocessed
- All outputs overwritten
- ~25 files/second (same as first run)

## What Gets Saved Where

| Output | Location | Content | Size vs HTML |
|--------|----------|---------|--------------|
| **Raw text** | `workspace/store/text/` | HTML tags removed only | ~60-70% |
| **Preprocessed** | `workspace/store/text-preprocessed/` | Boilerplate removed | ~40-50% |
| **README** | `workspace/store/readme/` | README text only | Varies |
| **Entities** | `workspace/store/entities/entities.tsv` | All entities + offsets | Small |

## Verification

```bash
# Check what was created
ls workspace/store/

# Count files
find workspace/store/text -type f | wc -l
find workspace/store/text-preprocessed -type f | wc -l
find workspace/store/readme -type f | wc -l

# Compare sizes
du -h workspace/store/text/
du -h workspace/store/text-preprocessed/
```

## Migration Notes

### If you already ran the extractor before:

**Old behavior**:
- Only preprocessed text was created
- No raw text option
- No skip behavior
- Had to manually delete files to reprocess

**New behavior**:
- Both raw and preprocessed text created
- Smart skip for existing files
- `--force` flag to overwrite
- Faster incremental processing

### To get raw text for existing extractions:

```bash
# Option 1: Use --force to regenerate everything
python -m extractor --force

# Option 2: Delete only text outputs and rerun
rm -rf workspace/store/text
python -m extractor
# (will skip preprocessed files, only generate raw text)
```

## FAQs

**Q: Which text should I use?**
- **Raw text**: When you need complete content (CSS, navigation, etc.)
- **Preprocessed**: For NLP, search indexing, clean analysis

**Q: How to reprocess just one file?**
```bash
# Delete its outputs first
rm workspace/store/text/00/02/{doc_id}.txt
rm workspace/store/text-preprocessed/00/02/{doc_id}.txt
# Then run
python -m extractor
```

**Q: Does --force delete files?**
- No, it overwrites them during processing
- Safe to use

**Q: What if I only want raw text?**
```bash
python -m extractor --preproc-out /dev/null
# (Don't do this, both are generated anyway)
```

**Q: How much disk space?**
- Raw text: ~60-70% of HTML size
- Preprocessed: ~40-50% of HTML size
- Both together: ~110% of HTML size
- Plan accordingly!

---

**Updated**: 2025-10-30
**Version**: 2.0 with raw text + force flag
