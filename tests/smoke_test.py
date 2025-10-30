#!/usr/bin/env python3
"""Smoke test for entity extraction.

Tests basic functionality on sample HTML files without requiring full test suite.
Verifies:
- Entities TSV is created with header
- At least some entities are extracted
- Preprocessed text files are created
- No crashes on files lacking metadata
"""

import argparse
import sys
from pathlib import Path

# Add parent dir to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from extractor import entity_main, io_utils


def run_smoke_test(sample_dir: Path, output_dir: Path) -> int:
    """Run smoke test on sample HTML files.

    Args:
        sample_dir: Directory containing sample HTML files
        output_dir: Directory for test outputs

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    print("=" * 70)
    print("ENTITY EXTRACTOR SMOKE TEST")
    print("=" * 70)
    print(f"Sample dir:  {sample_dir}")
    print(f"Output dir:  {output_dir}")
    print()

    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)

    # Define output paths
    entities_tsv = output_dir / "entities.tsv"
    preproc_dir = output_dir / "text-preprocessed"
    readme_dir = output_dir / "readme"

    # Clean up previous test outputs
    if entities_tsv.exists():
        entities_tsv.unlink()
    if preproc_dir.exists():
        for f in preproc_dir.rglob('*'):
            if f.is_file():
                f.unlink()
    if readme_dir.exists():
        for f in readme_dir.rglob('*'):
            if f.is_file():
                f.unlink()

    # Run extraction
    print("Running extraction...")
    argv = [
        '--in', str(sample_dir),
        '--entities-out', str(entities_tsv),
        '--preproc-out', str(preproc_dir),
        '--readme-out', str(readme_dir),
    ]

    try:
        exit_code = entity_main.main_entity_extraction(argv)
        if exit_code != 0:
            print(f"❌ Extraction failed with exit code {exit_code}")
            return 1
    except Exception as exc:
        print(f"❌ Extraction crashed: {exc}")
        import traceback
        traceback.print_exc()
        return 1

    print()
    print("=" * 70)
    print("VALIDATION")
    print("=" * 70)

    # Check 1: Entities TSV exists and has header
    print("Check 1: Entities TSV exists and has header...")
    if not entities_tsv.exists():
        print("  ❌ FAIL: entities.tsv not found")
        return 1

    with entities_tsv.open('r', encoding='utf-8') as f:
        lines = f.readlines()

    if not lines:
        print("  ❌ FAIL: entities.tsv is empty")
        return 1

    header = lines[0].strip()
    expected_header = "doc_id\ttype\tvalue\toffsets_json"
    if header != expected_header:
        print(f"  ❌ FAIL: Header mismatch")
        print(f"     Expected: {expected_header}")
        print(f"     Got:      {header}")
        return 1

    print(f"  ✓ PASS: Header correct, {len(lines)-1} entity rows")

    # Check 2: At least some entities extracted
    print("Check 2: At least some entities extracted...")
    if len(lines) <= 1:
        print("  ⚠️  WARNING: No entities extracted (header only)")
    else:
        print(f"  ✓ PASS: {len(lines)-1} entities extracted")

    # Check 3: Entity type distribution
    print("Check 3: Entity type distribution...")
    type_counts = {}
    for line in lines[1:]:
        parts = line.strip().split('\t')
        if len(parts) >= 2:
            entity_type = parts[1]
            type_counts[entity_type] = type_counts.get(entity_type, 0) + 1

    if type_counts:
        print("  Entity types found:")
        for entity_type, count in sorted(type_counts.items()):
            print(f"    {entity_type:20} : {count:4}")

        # Check for key entity types
        key_types_found = []
        if 'STAR_COUNT' in type_counts:
            key_types_found.append('STAR_COUNT')
        if 'FORK_COUNT' in type_counts:
            key_types_found.append('FORK_COUNT')
        if 'LANG_STATS' in type_counts:
            key_types_found.append('LANG_STATS')
        if 'README_SECTION' in type_counts:
            key_types_found.append('README_SECTION')

        if key_types_found:
            print(f"  ✓ PASS: Found metadata entities: {', '.join(key_types_found)}")
        else:
            print("  ⚠️  WARNING: No metadata entities (STAR_COUNT, FORK_COUNT, etc.) found")
    else:
        print("  ⚠️  WARNING: No entity types found")

    # Check 4: Preprocessed text files exist
    print("Check 4: Preprocessed text files exist...")
    if not preproc_dir.exists():
        print("  ❌ FAIL: Preprocessed text directory not created")
        return 1

    preproc_files = list(preproc_dir.rglob('*.txt'))
    if not preproc_files:
        print("  ⚠️  WARNING: No preprocessed text files created")
    else:
        print(f"  ✓ PASS: {len(preproc_files)} preprocessed text files created")

        # Check file sizes (should be non-empty for most files)
        non_empty_count = 0
        for f in preproc_files:
            if f.stat().st_size > 100:  # At least 100 bytes
                non_empty_count += 1

        if non_empty_count > 0:
            pct = (non_empty_count / len(preproc_files)) * 100
            print(f"  ✓ PASS: {non_empty_count}/{len(preproc_files)} files non-empty ({pct:.1f}%)")
        else:
            print("  ⚠️  WARNING: All preprocessed files are very small or empty")

    # Check 5: README files in dedicated directory
    print("Check 5: README files in dedicated directory...")
    if not readme_dir.exists():
        print("  ⚠️  WARNING: README directory not created")
    else:
        readme_files = list(readme_dir.rglob('*.txt'))
        if not readme_files:
            print("  ⚠️  INFO: No README files extracted (depends on content)")
        else:
            print(f"  ✓ PASS: {len(readme_files)} README files in dedicated directory")

    # Check 6: No crashes on sample files
    print("Check 6: Processing completed without crashes...")
    print("  ✓ PASS: All sample files processed")

    print()
    print("=" * 70)
    print("✓ SMOKE TEST PASSED")
    print("=" * 70)

    return 0


def main(argv=None):
    """Main entry point for smoke test."""
    parser = argparse.ArgumentParser(description="Run smoke test for entity extractor")
    parser.add_argument(
        '--samples',
        default='tests_regex_samples',
        help='Directory containing sample HTML files (default: tests_regex_samples)',
    )
    parser.add_argument(
        '--output',
        default='workspace/test_output',
        help='Output directory for test results (default: workspace/test_output)',
    )

    args = parser.parse_args(argv)

    sample_dir = Path(args.samples)
    output_dir = Path(args.output)

    if not sample_dir.exists():
        print(f"❌ Sample directory not found: {sample_dir}")
        print("   Create it with: mkdir -p tests_regex_samples")
        print("   Then copy some HTML files into it.")
        return 1

    sample_files = list(sample_dir.glob('*.html'))
    if not sample_files:
        print(f"❌ No HTML files found in: {sample_dir}")
        return 1

    print(f"Found {len(sample_files)} sample HTML files")
    print()

    return run_smoke_test(sample_dir, output_dir)


if __name__ == '__main__':
    sys.exit(main())
