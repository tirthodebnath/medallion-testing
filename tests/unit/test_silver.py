"""
Unit tests for the Silver layer.

Each transformation step is tested in isolation, plus a "compose" test
that wires them together via build_silver_claims().
"""
from datetime import date, datetime
from decimal import Decimal

import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DecimalType, StringType, StructField, StructType

from src.silver.transform import (
    cast_claims_types,
    filter_invalid_claims,
    enrich_with_business_fields,
    join_members,
    build_silver_claims,
)
from src.common.utils import classify_claim_amount


# ---------------------------------------------------------------------------
# classify_claim_amount — pure column function
# ---------------------------------------------------------------------------
class TestClassifyClaimAmount:
    """Boundary tests: bands are closed on the right edge."""

    @pytest.mark.parametrize("amount,expected", [
        (None,            "UNKNOWN"),
        (0,               "UNKNOWN"),
        (-50,             "UNKNOWN"),
        (1,               "LOW"),
        (100,             "LOW"),       # right edge of LOW
        ("100.00",        "LOW"),
        (100.01,          "MEDIUM"),    # just over -> next band
        (1000,            "MEDIUM"),    # right edge of MEDIUM
        (1000.01,         "HIGH"),
        (10000,           "HIGH"),      # right edge of HIGH
        (10000.01,        "JUMBO"),
        (1_000_000,       "JUMBO"),
    ])
    def test_band_boundaries(self, spark, amount, expected):
        # Explicit schema so None values can be created.
        schema = StructType([StructField("claim_amount", StringType(), True)])
        df = (
            spark.createDataFrame([(None if amount is None else str(amount),)], schema=schema)
                 .withColumn("claim_amount", F.col("claim_amount").cast("decimal(12,2)"))
        )
        out = df.withColumn("band", classify_claim_amount(F.col("claim_amount")))
        assert out.first()["band"] == expected


# ---------------------------------------------------------------------------
# cast_claims_types
# ---------------------------------------------------------------------------
class TestCastClaimsTypes:

    def test_string_date_becomes_date(self, spark):
        df = spark.createDataFrame([("2024-01-15", "100.00")],
                                   ["claim_date", "claim_amount"])
        out = cast_claims_types(df)
        assert isinstance(out.schema["claim_date"].dataType, DateType)
        assert out.first()["claim_date"] == date(2024, 1, 15)

    def test_string_amount_becomes_decimal_12_2(self, spark):
        df = spark.createDataFrame([("2024-01-15", "1234.56")],
                                   ["claim_date", "claim_amount"])
        out = cast_claims_types(df)
        assert isinstance(out.schema["claim_amount"].dataType, DecimalType)
        assert out.first()["claim_amount"] == Decimal("1234.56")

    def test_unparseable_date_yields_null(self, spark):
        df = spark.createDataFrame([("not-a-date", "100.00")],
                                   ["claim_date", "claim_amount"])
        out = cast_claims_types(df)
        assert out.first()["claim_date"] is None


# ---------------------------------------------------------------------------
# filter_invalid_claims
# ---------------------------------------------------------------------------
class TestFilterInvalidClaims:
    """Every invariant gets its own row in this single dataset."""

    @pytest.fixture
    def mixed_df(self, spark):
        # Each row hits exactly one failure mode (or is the happy-path row).
        return spark.createDataFrame(
            [
                ("C001", "M001", date(2024, 1, 15), Decimal("100.00")),   # OK
                (None,   "M001", date(2024, 1, 15), Decimal("100.00")),   # null claim_id
                ("C002", None,   date(2024, 1, 15), Decimal("100.00")),   # null member_id
                ("C003", "M001", None,              Decimal("100.00")),   # null date
                ("C004", "M001", date(2024, 1, 15), None),                # null amount
                ("C005", "M001", date(2024, 1, 15), Decimal("-50.00")),   # negative amount
            ],
            ["claim_id", "member_id", "claim_date", "claim_amount"],
        )

    def test_only_valid_row_survives(self, mixed_df):
        out = filter_invalid_claims(mixed_df)
        assert out.count() == 1
        assert out.first()["claim_id"] == "C001"

    def test_zero_amount_is_valid(self, spark):
        # Business rule: 0 is allowed (e.g., adjusted-to-zero claims), only <0 is not.
        df = spark.createDataFrame(
            [("C001", "M001", date(2024, 1, 15), Decimal("0.00"))],
            ["claim_id", "member_id", "claim_date", "claim_amount"],
        )
        assert filter_invalid_claims(df).count() == 1


# ---------------------------------------------------------------------------
# enrich_with_business_fields
# ---------------------------------------------------------------------------
class TestEnrich:

    def test_is_denied_true_for_denied_status(self, spark):
        df = spark.createDataFrame(
            [("DENIED", Decimal("100.00")), ("PAID", Decimal("100.00"))],
            ["status", "claim_amount"],
        )
        out = enrich_with_business_fields(df).collect()
        assert out[0]["is_denied"] is True
        assert out[1]["is_denied"] is False

    def test_is_denied_case_insensitive(self, spark):
        df = spark.createDataFrame([("denied", Decimal("10"))],
                                   ["status", "claim_amount"])
        assert enrich_with_business_fields(df).first()["is_denied"] is True

    def test_band_is_attached(self, spark):
        df = spark.createDataFrame([("PAID", Decimal("250.00"))],
                                   ["status", "claim_amount"])
        out = enrich_with_business_fields(df).first()
        assert out["claim_amount_band"] == "MEDIUM"


# ---------------------------------------------------------------------------
# join_members — important: LEFT join, never drops claims
# ---------------------------------------------------------------------------
class TestJoinMembers:

    def test_left_join_keeps_orphan_claims(self, spark):
        claims = spark.createDataFrame(
            [("C001", "M001"), ("C002", "M999")],  # M999 not in members
            ["claim_id", "member_id"],
        )
        members = spark.createDataFrame(
            [("M001", "GOLD", "NY")],
            ["member_id", "plan_type", "state"],
        )
        out = join_members(claims, members).orderBy("claim_id").collect()
        assert len(out) == 2
        assert out[0]["plan_type"] == "GOLD"
        assert out[1]["plan_type"] is None      # orphan claim kept, plan null

    def test_join_does_not_duplicate_rows(self, spark):
        """Members must be deduped on member_id upstream. We assume it here."""
        claims = spark.createDataFrame([("C001", "M001")], ["claim_id", "member_id"])
        members = spark.createDataFrame(
            [("M001", "GOLD", "NY")],
            ["member_id", "plan_type", "state"],
        )
        assert join_members(claims, members).count() == 1


# ---------------------------------------------------------------------------
# build_silver_claims — composition test
# ---------------------------------------------------------------------------
class TestBuildSilverClaims:
    """
    End-to-end Silver build. Uses the shared bronze_*_df fixtures from
    conftest. Critical assertions:
      - All-good rows make it through.
      - Output schema matches the Silver contract column set.
      - Enriched columns are populated.
    """

    def test_row_count_matches_valid_input(self, bronze_claims_df, bronze_members_df):
        # All 4 fixture claims are valid -> all 4 should survive.
        out = build_silver_claims(bronze_claims_df, bronze_members_df)
        assert out.count() == 4

    def test_output_columns(self, bronze_claims_df, bronze_members_df):
        out = build_silver_claims(bronze_claims_df, bronze_members_df)
        expected_cols = {
            "claim_id", "member_id", "provider_id", "claim_date",
            "claim_amount", "status", "denial_code", "is_denied",
            "claim_amount_band", "plan_type", "state",
            "_ingestion_timestamp",
        }
        assert set(out.columns) == expected_cols

    def test_member_join_populated(self, bronze_claims_df, bronze_members_df):
        out = build_silver_claims(bronze_claims_df, bronze_members_df)
        m001 = out.filter("claim_id = 'C001'").first()
        assert m001["plan_type"] == "GOLD"
        assert m001["state"] == "NY"

    def test_denial_propagates(self, bronze_claims_df, bronze_members_df):
        out = build_silver_claims(bronze_claims_df, bronze_members_df)
        denied = out.filter("claim_id = 'C002'").first()
        assert denied["is_denied"] is True
        assert denied["denial_code"] == "D01"

    def test_dedupe_keeps_latest_ingestion(self, spark, make_bronze_claim, make_bronze_member,
                                           bronze_claims_schema, bronze_members_schema):
        """
        Two rows with the same claim_id but different ingestion timestamps
        and DIFFERENT amounts. The one with the later timestamp should win.
        """
        older = make_bronze_claim(
            claim_id="C001",
            claim_amount="100.00",
            ingestion_ts=datetime(2024, 1, 16, 10, 0, 0),
        )
        newer = make_bronze_claim(
            claim_id="C001",
            claim_amount="250.00",   # corrected amount in later file
            ingestion_ts=datetime(2024, 1, 17, 10, 0, 0),
        )
        bronze_claims = spark.createDataFrame([older, newer], schema=bronze_claims_schema)
        bronze_members = spark.createDataFrame([make_bronze_member("M001")], schema=bronze_members_schema)

        out = build_silver_claims(bronze_claims, bronze_members)
        assert out.count() == 1
        assert out.first()["claim_amount"] == Decimal("250.00")
