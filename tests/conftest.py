"""
Shared pytest fixtures for the entire test suite.

Key design choices:
- A SINGLE session-scoped SparkSession is reused across all tests. Spinning
  one up costs ~5-10s; doing it per-test would make the suite unbearable.
- Default `spark` is a PLAIN Spark session (no Delta) so unit, regression
  and DQ tests don't need the Delta JARs to be on the classpath.
- A separate `spark_delta` session-scoped fixture configures Delta via
  `configure_spark_with_delta_pip` for integration tests that actually
  read/write Delta tables. This downloads Delta JARs from Maven on first
  use; on a Databricks cluster Delta is already present so the helper is
  a no-op.
- `spark.sql.shuffle.partitions=4` keeps shuffle small on the driver-only
  local cluster used by pytest. (Default 200 is wasteful for tiny tests.)
"""
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType, StructField, StructType, TimestampType,
)


# ---------------------------------------------------------------------------
# Schemas used by the row builders below
# Including the audit columns makes the test fixtures look exactly like
# Bronze tables on disk -- so what we test is what the pipeline sees.
# ---------------------------------------------------------------------------
_BRONZE_CLAIMS_WITH_AUDIT_SCHEMA = StructType([
    StructField("claim_id",             StringType(),    True),
    StructField("member_id",            StringType(),    True),
    StructField("provider_id",          StringType(),    True),
    StructField("claim_date",           StringType(),    True),
    StructField("claim_amount",         StringType(),    True),
    StructField("status",               StringType(),    True),
    StructField("denial_code",          StringType(),    True),
    StructField("_ingestion_timestamp", TimestampType(), True),
    StructField("_source_file",         StringType(),    True),
])

_BRONZE_MEMBERS_WITH_AUDIT_SCHEMA = StructType([
    StructField("member_id",            StringType(),    True),
    StructField("member_name",          StringType(),    True),
    StructField("plan_type",            StringType(),    True),
    StructField("enrollment_date",      StringType(),    True),
    StructField("state",                StringType(),    True),
    StructField("_ingestion_timestamp", TimestampType(), True),
    StructField("_source_file",         StringType(),    True),
])


import os


def _on_databricks() -> bool:
    """True when running inside a Databricks notebook / job."""
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


# ---------------------------------------------------------------------------
# SparkSession (plain — no Delta)
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """
    Session-scoped SparkSession.

    On Databricks: returns the existing Spark Connect session (serverless).
    Locally:       spins up a standalone local[2] session for pytest.
    """
    if _on_databricks():
        spark = SparkSession.builder.getOrCreate()
        yield spark
        # Don't stop — the session is owned by the notebook, not by us.
    else:
        spark = (
            SparkSession.builder
            .appName("medallion-tests")
            .master("local[2]")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        yield spark
        spark.stop()


@pytest.fixture(scope="session")
def spark_delta() -> SparkSession:
    """
    Spark session with Delta Lake enabled. Used by integration tests.

    On Databricks, Delta is built into the runtime — we just return the
    existing session. Locally, uses `configure_spark_with_delta_pip` to
    pull the right JARs from Maven.
    """
    if _on_databricks():
        yield SparkSession.builder.getOrCreate()
        return

    try:
        from delta import configure_spark_with_delta_pip
    except ImportError:
        pytest.skip("delta-spark is not installed; skipping Delta-backed tests.")

    builder = (
        SparkSession.builder
        .appName("medallion-tests-delta")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


# ---------------------------------------------------------------------------
# In-memory sample data builders (used by many unit tests)
#
# Why builders, not constants?
# Tests often need to tweak one row (e.g. set claim_amount to -50 to verify
# the invalid filter fires). A builder lets each test express only the delta
# it cares about, keeping intent in the test rather than at module top-level.
# ---------------------------------------------------------------------------
def _bronze_claims_row(
    claim_id="C001",
    member_id="M001",
    provider_id="P001",
    claim_date="2024-01-15",
    claim_amount="250.00",
    status="PAID",
    denial_code=None,
    ingestion_ts=None,
    source_file="test.csv",
):
    """Build one bronze-shaped row tuple (matches _BRONZE_CLAIMS_WITH_AUDIT_SCHEMA)."""
    return (
        claim_id,
        member_id,
        provider_id,
        claim_date,
        claim_amount,
        status,
        denial_code,
        ingestion_ts or datetime(2024, 1, 16, 10, 0, 0),
        source_file,
    )


def _bronze_members_row(
    member_id="M001",
    member_name="Alice",
    plan_type="GOLD",
    enrollment_date="2022-01-01",
    state="NY",
    ingestion_ts=None,
    source_file="members.csv",
):
    return (
        member_id,
        member_name,
        plan_type,
        enrollment_date,
        state,
        ingestion_ts or datetime(2024, 1, 16, 10, 0, 0),
        source_file,
    )


@pytest.fixture
def bronze_claims_df(spark):
    """A small, realistic bronze claims DataFrame covering common edge cases."""
    rows = [
        _bronze_claims_row("C001", "M001", "P001", "2024-01-15", "250.00",  "PAID",   None),
        _bronze_claims_row("C002", "M001", "P002", "2024-01-20", "1500.00", "DENIED", "D01"),
        _bronze_claims_row("C003", "M002", "P001", "2024-02-05", "75.50",   "PAID",   None),
        _bronze_claims_row("C004", "M003", "P003", "2024-02-10", "12000.00", "PAID",  None),
    ]
    return spark.createDataFrame(rows, schema=_BRONZE_CLAIMS_WITH_AUDIT_SCHEMA)


@pytest.fixture
def bronze_members_df(spark):
    rows = [
        _bronze_members_row("M001", "Alice",   "GOLD",    "2022-01-01", "NY"),
        _bronze_members_row("M002", "Bob",     "SILVER",  "2023-06-15", "CA"),
        _bronze_members_row("M003", "Charlie", "BRONZE",  "2024-01-01", "TX"),
    ]
    return spark.createDataFrame(rows, schema=_BRONZE_MEMBERS_WITH_AUDIT_SCHEMA)


# Expose schemas + row builders so tests that need custom rows can use them.
@pytest.fixture
def bronze_claims_schema():
    return _BRONZE_CLAIMS_WITH_AUDIT_SCHEMA


@pytest.fixture
def bronze_members_schema():
    return _BRONZE_MEMBERS_WITH_AUDIT_SCHEMA


@pytest.fixture
def make_bronze_claim():
    return _bronze_claims_row


@pytest.fixture
def make_bronze_member():
    return _bronze_members_row


# ---------------------------------------------------------------------------
# Filesystem helpers
# ---------------------------------------------------------------------------
@pytest.fixture
def tmp_warehouse(tmp_path: Path) -> Path:
    """A throwaway warehouse dir for integration tests that need Delta paths."""
    wh = tmp_path / "warehouse"
    wh.mkdir()
    return wh


@pytest.fixture
def golden_data_dir() -> Path:
    """Filesystem path to the regression golden datasets."""
    return Path(__file__).parent / "regression" / "golden_data"
