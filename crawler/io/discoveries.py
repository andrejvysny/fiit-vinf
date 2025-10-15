"""LEGACY: DiscoveriesWriter removed.

The discoveries spool writer was part of the separated producer/consumer
architecture. Unified mode no longer emits discoveries to a spool. Importing
this module will raise an ImportError to avoid accidental usage.
"""

raise ImportError(
    "Legacy DiscoveriesWriter removed. Use unified mode metadata writer: crawler/io/metadata_writer.py"
)
