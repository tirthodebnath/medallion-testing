"""
Unit tests for the Gold aggregations.

We build small, hand-crafted Silver-shaped DataFrames so each aggregation
behavior is unambiguous.
"""
from datetime import date, datetime
from decimal import Decimal

import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import Row

from pyspark.sql.types import (
    BooleanType, DateType, DecimalType, StringType, StructField, StructType,
)

from src.gold.aggregate import (
    build_monthly_claims_kpi,
    build_member_lifetime_kpi,
)


# Explicit schema lets us createDataFrame even with zero rows.
_SILVER_FOR_GOLD_SCHEMA = StructType([
    StructField("claim_id",     StringType(),      False),
    StructField("member_id",    StringType(),      False),
    StructField("claim_date",   DateType(),        False),
    StructField("claim_amount", DecimalType(12, 2), False),
    StructField("status",       StringType(),      False),
    StructField("is_denied",    BooleanType(),     False),
])


# ---------------------------------------------------------------------------
# Helper: build a silver-shaped DataFrame from row dicts
# ---------------------------------------------------------------------------
def _silver(spark, rows):
    """Create a DataFrame matching the Silver column set used by gold."""
    data = [
        (
            r["claim_id"],
            r["member_id"],
            r["claim_date"],
            Decimal(r["claim_amount"]),
            r["status"],
            r.get("is_denied", r["status"] == "DENIED"),
        )
        for r in rows
    ]
    return spark.createDataFrame(data, schema=_SILVER_FOR_GOLD_SCHEMA)


# ---------------------------------------------------------------------------
# Monthly KPI
# ---------------------------------------------------------------------------
class TestMonthlyClaimsKPI:

    def test_groups_by_month_and_status(self, spark):
        df = _silver(spark, [
            {"claim_id": "C1", "member_id": "M1", "claim_date": date(2024, 1, 5),
             "claim_amount": "100.00", "status": "PAID"},
            {"claim_id": "C2", "member_id": "M1", "claim_date": date(2024, 1, 25),
             "claim_amount": "200.00", "status": "PAID"},
            {"claim_id": "C3", "member_id": "M2", "claim_date": date(2024, 1, 10),
             "claim_amount": "150.00", "status": "DENIED"},
            {"claim_id": "C4", "member_id": "M3", "claim_date": date(2024, 2, 1),
             "claim_amount": "500.00", "status": "PAID"},
        ])
        out = build_monthly_claims_kpi(df).collect()

        as_dict = {(r["claim_year_month"], r["status"]): r for r in out}
        assert as_dict[("2024-01", "PAID")]["claim_count"] == 2
        assert as_dict[("2024-01", "PAID")]["total_claim_amount"] == Decimal("300.00")
        assert as_dict[("2024-01", "DENIED")]["claim_count"] == 1
        assert as_dict[("2024-02", "PAID")]["claim_count"] == 1

    def test_avg_amount_is_mean(self, spark):
        df = _silver(spark, [
            {"claim_id": "C1", "member_id": "M1", "claim_date": date(2024, 1, 5),
             "claim_amount": "100.00", "status": "PAID"},
            {"claim_id": "C2", "member_id": "M1", "claim_date": date(2024, 1, 25),
             "claim_amount": "300.00", "status": "PAID"},
        ])
        out = build_monthly_claims_kpi(df).first()
        assert out["avg_claim_amount"] == Decimal("200.0000")

    def test_empty_input_yields_empty_output(self, spark):
        df = _silver(spark, [])
        assert build_monthly_claims_kpi(df).count() == 0


# ---------------------------------------------------------------------------
# Member lifetime KPI
# ---------------------------------------------------------------------------
class TestMemberLifetimeKPI:

    def test_total_paid_excludes_denied(self, spark):
        """Denied claims must NOT contribute to total_paid_amount."""
        df = _silver(spark, [
            {"claim_id": "C1", "member_id": "M1", "claim_date": date(2024, 1, 5),
             "claim_amount": "100.00", "status": "PAID",   "is_denied": False},
            {"claim_id": "C2", "member_id": "M1", "claim_date": date(2024, 1, 25),
             "claim_amount": "500.00", "status": "DENIED", "is_denied": True},
        ])
        out = build_member_lifetime_kpi(df).first()

        assert out["member_id"] == "M1"
        assert out["total_claims"] == 2
        assert out["total_paid_amount"] == Decimal("100.00")
        assert out["total_denied_claims"] == 1

    def test_first_and_last_claim_date(self, spark):
        df = _silver(spark, [
            {"claim_id": "C1", "member_id": "M1", "claim_date": date(2024, 1, 5),
             "claim_amount": "100.00", "status": "PAID",   "is_denied": False},
            {"claim_id": "C2", "member_id": "M1", "claim_date": date(2024, 3, 12),
             "claim_amount": "100.00", "status": "PAID",   "is_denied": False},
            {"claim_id": "C3", "member_id": "M1", "claim_date": date(2024, 2, 7),
             "claim_amount": "100.00", "status": "PAID",   "is_denied": False},
        ])
        out = build_member_lifetime_kpi(df).first()
        assert out["first_claim_date"] == date(2024, 1, 5)
        assert out["last_claim_date"] == date(2024, 3, 12)

    def test_one_row_per_member(self, spark):
        df = _silver(spark, [
            {"claim_id": "C1", "member_id": "M1", "claim_date": date(2024, 1, 5),
             "claim_amount": "100.00", "status": "PAID",   "is_denied": False},
            {"claim_id": "C2", "member_id": "M2", "claim_date": date(2024, 1, 25),
             "claim_amount": "200.00", "status": "PAID",   "is_denied": False},
            {"claim_id": "C3", "member_id": "M1", "claim_date": date(2024, 2, 1),
             "claim_amount": "300.00", "status": "PAID",   "is_denied": False},
        ])
        out = build_member_lifetime_kpi(df).collect()
        assert len(out) == 2  # M1 and M2

    def test_member_with_all_denied(self, spark):
        df = _silver(spark, [
            {"claim_id": "C1", "member_id": "M1", "claim_date": date(2024, 1, 5),
             "claim_amount": "100.00", "status": "DENIED", "is_denied": True},
            {"claim_id": "C2", "member_id": "M1", "claim_date": date(2024, 2, 5),
             "claim_amount": "200.00", "status": "DENIED", "is_denied": True},
        ])
        out = build_member_lifetime_kpi(df).first()
        assert out["total_paid_amount"] == Decimal("0.00")
        assert out["total_denied_claims"] == 2
