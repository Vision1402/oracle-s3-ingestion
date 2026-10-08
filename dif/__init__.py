"""DIF: a small, config-driven Data Ingestion Framework.

Moves Oracle tables to an S3 raw layer as partitioned Parquet files.
Each table is described by a YAML file in config/tables/; the framework
does the rest (extract, convert types, write, validate, record watermark).
"""

__version__ = "0.1.0"
