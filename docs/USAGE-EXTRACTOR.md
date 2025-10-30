# Entity Extractor - Quick Usage Guide

## TL;DR

```bash
# Extract everything from all HTML files (uses defaults)
python -m extractor

# Test with 10 files first
python -m extractor --sample 10 --verbose
```

## Default Behavior

Running `python -m extractor` without arguments will:

✓ Read HTML from: `workspace/store/html/`
✓ Write entities to: `workspace/store/entities/entities.tsv`
✓ Write preprocessed text to: `workspace/store/text-preprocessed/`
✓ Write README files to: `workspace/store/readme/`
✓ Process **ALL** HTML files found

## Common Usage Patterns

### 1. Quick Test (Recommended First Step)

```bash
# Process just 10 files to verify everything works
python -m extractor --sample 10 --verbose
```

**What to check:**
- `workspace/store/entities/entities.tsv` exists
- `workspace/store/text-preprocessed/` has .txt files
- `workspace/store/readme/` has README files (if any detected)
- No errors in output

### 2. Larger Test

```bash
# Process 100 files to get representative sample
python -m extractor --sample 100 --verbose
```

### 3. Full Extraction

```bash
# Process all ~28,000 files (takes ~15-20 minutes)
python -m extractor --verbose
```

### 4. Dry Run (See What Would Be Processed)

```bash
# List files without processing
python -m extractor --dry-run
```

### 5. Custom Paths

```bash
python -m extractor \
  --in /path/to/html/files \
  --entities-out /path/to/output/entities.tsv \
  --preproc-out /path/to/preprocessed/text \
  --readme-out /path/to/readme/files
```

## Output Files

After running, you'll have:

```
workspace/store/
├── entities/
│   └── entities.tsv          # All extracted entities with offsets
├── text-preprocessed/
│   ├── 00/
│   │   ├── 00/
│   │   │   └── {doc_id}.txt  # Cleaned text for each HTML
│   │   └── ...
│   └── ...
└── readme/
    ├── 00/
    │   ├── 00/
    │   │   └── {doc_id}.txt  # README files only
    │   └── ...
    └── ...
```

## Inspecting Results

```bash
# View entity statistics
cut -f2 workspace/store/entities/entities.tsv | sort | uniq -c | sort -rn

# Count files processed
wc -l workspace/store/entities/entities.tsv

# View sample entities
head -20 workspace/store/entities/entities.tsv

# Count README files found
find workspace/store/readme -type f | wc -l

# View a sample README
find workspace/store/readme -name "*.txt" | head -1 | xargs cat
```

## Troubleshooting

### No outputs created?

```bash
# Check if HTML files exist
ls -la workspace/store/html/ | head

# Try with verbose logging
python -m extractor --sample 1 --verbose
```

### Want to re-run?

```bash
# Clean up previous outputs
rm -rf workspace/store/entities workspace/store/text-preprocessed workspace/store/readme

# Run again
python -m extractor --sample 10
```

### How to run smoke test?

```bash
python tests/smoke_test.py
```

## Performance Expectations

| Files | Expected Time | Command |
|-------|---------------|---------|
| 10 | ~0.4 seconds | `python -m extractor --sample 10` |
| 100 | ~4 seconds | `python -m extractor --sample 100` |
| 1,000 | ~40 seconds | `python -m extractor --limit 1000` |
| 28,353 (all) | ~18-20 minutes | `python -m extractor` |

Speed: ~25-30 files/second on typical hardware.

## Getting Help

```bash
# Show all options
python -m extractor --help

# Read full documentation
cat README-EXTRACTOR.md
```

## Integration with Existing Text Files

**Important:** The new extractor does NOT modify existing `workspace/store/text/` files created by the old simple extractor. They remain untouched.

The new extractor creates:
- **text-preprocessed/** - Enhanced version with boilerplate removal
- **entities/** - Structured entity data
- **readme/** - Dedicated README files

Both can coexist peacefully.

## What Gets Extracted?

From **each HTML file**, the extractor identifies:

- ⭐ **Star counts** (STAR_COUNT)
- 🍴 **Fork counts** (FORK_COUNT)
- 📊 **Language statistics** (LANG_STATS) - JSON with percentages
- 📖 **README text** (README_SECTION) - Main documentation
- 📜 **Licenses** (LICENSE) - MIT, Apache, GPL, etc.
- 🏷️ **Topics** (TOPIC) - Repository tags
- 📦 **Imports** (IMPORT) - Python, JS, Rust, C/C++, Go, PHP
- 🔗 **URLs** (URL) - All HTTP/HTTPS links
- 🔖 **Issue refs** (ISSUE_REF) - #123, owner/repo#123
- 🔢 **Versions** (VERSION) - Semantic versions
- 📧 **Emails** (EMAIL) - Contact addresses
- 💻 **Code languages** (LANG) - From code blocks

Plus **byte offsets** for every match, stored as JSON in the TSV.

## Next Steps

1. **Test**: `python -m extractor --sample 10 --verbose`
2. **Verify**: Check output files exist and look reasonable
3. **Run full**: `python -m extractor --verbose`
4. **Analyze**: Use the TSV for your IR/NLP tasks

---

**Last Updated:** 2025-10-30
**Full Docs:** `README-EXTRACTOR.md`
**Smoke Test:** `python tests/smoke_test.py`
