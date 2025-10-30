"""Entry point for running extractor as a module.

This is the unified entrypoint for the entity extraction pipeline.
Running without arguments will use default paths and extract all entities.

Usage:
    python -m extractor                    # Run with defaults
    python -m extractor --sample 10        # Test with 10 files
    python -m extractor --help             # Show all options
"""
from extractor.entity_main import main_entity_extraction

if __name__ == "__main__":
    raise SystemExit(main_entity_extraction())
