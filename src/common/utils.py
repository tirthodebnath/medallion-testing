"""
Pure helper functions shared across layers.

These are deliberately kept side-effect-free so unit tests can target them
directly without needing tables, paths, or dbutils.
"""
from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F


# ---------------------------------------------------------------------------
# Column-level helpers (return Column expressions)
# ---------------------------------------------------------------------------
def classify_claim_amount(amount_col: Column) -> Column:
    """
    Bucket a numeric claim amount into business-defined bands.

    LOW    : (0,     100]
    MEDIUM : (100,   1_000]
    HIGH   : (1_000, 10_000]
    JUMBO  : (10_000, +inf)
    UNKNOWN: null / non-positive
    """
    return (
        F.when(amount_col.isNull() | (amount_col <= 0), F.lit("UNKNOWN"))
         .when(amount_col <= 100,    F.lit("LOW"))
         .when(amount_col <= 1000,   F.lit("MEDIUM"))
         .when(amount_col <= 10000,  F.lit("HIGH"))
         .otherwise(F.lit("JUMBO"))
    )


def is_denied_flag(status_col: Column) -> Column:
    """Boolean flag for denied claims (case-insensitive)."""
    return F.upper(status_col) == F.lit("DENIED")


# ---------------------------------------------------------------------------
# DataFrame-level helpers
# ---------------------------------------------------------------------------
def with_audit_columns(df: DataFrame, source_file: str) -> DataFrame:
    """Attach ingestion audit columns used across all bronze tables."""
    return (
        df.withColumn("_ingestion_timestamp", F.current_timestamp())
          .withColumn("_source_file", F.lit(source_file))
    )


def dedupe_latest(df: DataFrame, key_cols: list, order_col: str) -> DataFrame:
    """
    Keep the latest record per key based on `order_col` (descending).

    Used to dedupe claims that arrive multiple times in source files
    (e.g., the same claim_id appearing in both today's and yesterday's drop).
    """
    from pyspark.sql.window import Window
    w = Window.partitionBy(*key_cols).orderBy(F.col(order_col).desc())
    return (
        df.withColumn("_rn", F.row_number().over(w))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )
