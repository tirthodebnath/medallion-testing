"""
Unit tests for the Bronze layer.

Bronze is intentionally thin (read + audit columns), so the tests focus on:
- Audit columns are attached correctly.
- Schema matches the contract regardless of input row count.
- Empty input is handled (zero-row DataFrame, not exception).
"""
from datetime import datetime

import pytest
from chispa.schema_comparer import assert_schema_equality
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType

from src.bronze.ingest import ingest_claims, ingest_members
from src.common.schemas import CLAIMS_RAW_SCHEMA, MEMBERS_RAW_SCHEMA
from src.common.utils import with_audit_columns


class TestAuditColumns:
    """`with_audit_columns` is the workhorse of bronze — test it directly."""

    def test_adds_both_audit_columns(self, spark):
        df = spark.createDataFrame([("C001",)], ["claim_id"])
        out = with_audit_columns(df, source_file="landing/claims_2024-01-15.csv")

        assert "_ingestion_timestamp" in out.columns
        assert "_source_file" in out.columns

    def test_source_file_is_literal(self, spark):
        df = spark.createDataFrame([("C001",), ("C002",)], ["claim_id"])
        out = with_audit_columns(df, source_file="landing/claims.csv")

        values = {r["_source_file"] for r in out.collect()}
        assert values == {"landing/claims.csv"}

    def test_ingestion_timestamp_is_timestamp_type(self, spark):
        df = spark.createDataFrame([("C001",)], ["claim_id"])
        out = with_audit_columns(df, source_file="x.csv")

        ts_field = [f for f in out.schema.fields if f.name == "_ingestion_timestamp"][0]
        assert isinstance(ts_field.dataType, TimestampType)

    def test_preserves_original_columns(self, spark):
        df = spark.createDataFrame([("C001", "M001")], ["claim_id", "member_id"])
        out = with_audit_columns(df, source_file="x.csv")
        assert {"claim_id", "member_id"}.issubset(set(out.columns))


class TestIngestClaimsCSV:
    """Ingest from real CSV files written to tmp_path."""

    def test_ingest_reads_all_rows(self, spark, tmp_path, spark_path):
        csv = tmp_path / "claims.csv"
        csv.write_text(
            "claim_id,member_id,provider_id,claim_date,claim_amount,status,denial_code\n"
            "C001,M001,P001,2024-01-15,250.00,PAID,\n"
            "C002,M001,P002,2024-01-20,1500.00,DENIED,D01\n"
        )

        df = ingest_claims(spark, spark_path(csv), source_format="csv")
        assert df.count() == 2

    def test_ingest_applies_bronze_schema(self, spark, tmp_path, spark_path):
        csv = tmp_path / "claims.csv"
        csv.write_text(
            "claim_id,member_id,provider_id,claim_date,claim_amount,status,denial_code\n"
            "C001,M001,P001,2024-01-15,250.00,PAID,\n"
        )
        df = ingest_claims(spark, spark_path(csv))

        # All declared columns of CLAIMS_RAW_SCHEMA must be StringType in bronze.
        for f in CLAIMS_RAW_SCHEMA.fields:
            field = df.schema[f.name]
            assert isinstance(field.dataType, StringType), \
                f"Column {f.name} should be StringType in bronze, got {field.dataType}"

    def test_ingest_attaches_audit_columns(self, spark, tmp_path, spark_path):
        csv = tmp_path / "claims.csv"
        csv.write_text("claim_id,member_id,provider_id,claim_date,claim_amount,status,denial_code\n"
                       "C001,M001,P001,2024-01-15,250.00,PAID,\n")
        df = ingest_claims(spark, spark_path(csv))
        assert "_ingestion_timestamp" in df.columns
        assert "_source_file" in df.columns

    def test_ingest_empty_file_returns_zero_rows(self, spark, tmp_path, spark_path):
        csv = tmp_path / "empty.csv"
        csv.write_text("claim_id,member_id,provider_id,claim_date,claim_amount,status,denial_code\n")
        df = ingest_claims(spark, spark_path(csv))
        assert df.count() == 0
        # But schema is still correct
        assert "_ingestion_timestamp" in df.columns


class TestIngestMembers:

    def test_ingest_members_csv(self, spark, tmp_path, spark_path):
        csv = tmp_path / "members.csv"
        csv.write_text(
            "member_id,member_name,plan_type,enrollment_date,state\n"
            "M001,Alice,GOLD,2022-01-01,NY\n"
        )
        df = ingest_members(spark, spark_path(csv))
        assert df.count() == 1
        assert df.first()["member_name"] == "Alice"
