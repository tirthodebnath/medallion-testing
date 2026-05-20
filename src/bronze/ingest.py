"""
Bronze layer: land raw source data verbatim, with audit columns.

The contract is intentionally minimal — Bronze should preserve source data
as-is so we can replay Silver from it without re-reading the upstream system.
"""
from pyspark.sql import DataFrame, SparkSession

from src.common.schemas import CLAIMS_RAW_SCHEMA, MEMBERS_RAW_SCHEMA
from src.common.utils import with_audit_columns


def ingest_claims(
    spark: SparkSession,
    source_path: str,
    source_format: str = "csv",
) -> DataFrame:
    """
    Read raw claims from `source_path` into the Bronze contract.

    Parameters
    ----------
    spark         : active SparkSession.
    source_path   : path or glob to read.
    source_format : 'csv' or 'json' (driven by upstream landing).
    """
    reader = spark.read.format(source_format).schema(CLAIMS_RAW_SCHEMA)
    if source_format == "csv":
        reader = reader.option("header", "true")
    df = reader.load(source_path)
    return with_audit_columns(df, source_file=source_path)


def ingest_members(
    spark: SparkSession,
    source_path: str,
    source_format: str = "csv",
) -> DataFrame:
    """Read raw members reference data into the Bronze contract."""
    reader = spark.read.format(source_format).schema(MEMBERS_RAW_SCHEMA)
    if source_format == "csv":
        reader = reader.option("header", "true")
    df = reader.load(source_path)
    return with_audit_columns(df, source_file=source_path)


def write_bronze(df: DataFrame, target_table: str) -> None:
    """
    Write the bronze DataFrame to a Delta table (append).

    Kept as a thin wrapper so tests can monkeypatch it cleanly.
    """
    (df.write
       .format("delta")
       .mode("append")
       .saveAsTable(target_table))
