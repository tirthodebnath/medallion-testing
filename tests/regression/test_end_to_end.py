"""
End-to-end regression tests.

Strategy
--------
1. Load fixed JSON golden INPUTS (representing what Bronze would hold).
2. Run the full pipeline.
3. Load fixed JSON golden EXPECTED OUTPUTS and compare with chispa.

What this catches
-----------------
- Any change in transformation logic that shifts the output of a known input.
- Schema drift (column added/removed/reordered).
- Type changes (e.g. someone switching decimal(18,2) -> double).

Regenerating the goldens
------------------------
If a change is INTENDED, regenerate the expected_*.json files via the
helper at the bottom of this file (run it as a script, then commit the diff).
The PR review of that diff is the human checkpoint.
"""
import json
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F

from src.common.schemas import (
    CLAIMS_RAW_SCHEMA,
    MEMBERS_RAW_SCHEMA,
    MONTHLY_CLAIMS_KPI_SCHEMA,
    MEMBER_LIFETIME_KPI_SCHEMA,
)
from src.common.utils import with_audit_columns
from src.pipeline import run_pipeline


# ---------------------------------------------------------------------------
# Loaders for golden datasets
# ---------------------------------------------------------------------------
def _load_json(path: Path):
    with open(path) as f:
        return json.load(f)


def _bronze_claims_from_golden(spark, golden_data_dir):
    rows = _load_json(golden_data_dir / "bronze_claims.json")
    df = spark.createDataFrame(rows, schema=CLAIMS_RAW_SCHEMA)
    return with_audit_columns(df, source_file="golden/bronze_claims.json")


def _bronze_members_from_golden(spark, golden_data_dir):
    rows = _load_json(golden_data_dir / "bronze_members.json")
    df = spark.createDataFrame(rows, schema=MEMBERS_RAW_SCHEMA)
    return with_audit_columns(df, source_file="golden/bronze_members.json")


def _expected_monthly_df(spark, golden_data_dir):
    raw = _load_json(golden_data_dir / "expected_monthly_kpi.json")
    rows = [
        Row(
            claim_year_month=r["claim_year_month"],
            status=r["status"],
            claim_count=int(r["claim_count"]),
            total_claim_amount=Decimal(r["total_claim_amount"]),
            avg_claim_amount=Decimal(r["avg_claim_amount"]),
        )
        for r in raw
    ]
    return spark.createDataFrame(rows, schema=MONTHLY_CLAIMS_KPI_SCHEMA)


def _expected_member_df(spark, golden_data_dir):
    raw = _load_json(golden_data_dir / "expected_member_kpi.json")
    rows = [
        Row(
            member_id=r["member_id"],
            total_claims=int(r["total_claims"]),
            total_paid_amount=Decimal(r["total_paid_amount"]),
            total_denied_claims=int(r["total_denied_claims"]),
            first_claim_date=date.fromisoformat(r["first_claim_date"]),
            last_claim_date=date.fromisoformat(r["last_claim_date"]),
        )
        for r in raw
    ]
    return spark.createDataFrame(rows, schema=MEMBER_LIFETIME_KPI_SCHEMA)


# ---------------------------------------------------------------------------
# Regression tests
# ---------------------------------------------------------------------------
class TestEndToEndRegression:

    @pytest.fixture
    def outputs(self, spark, golden_data_dir):
        bronze_claims  = _bronze_claims_from_golden(spark, golden_data_dir)
        bronze_members = _bronze_members_from_golden(spark, golden_data_dir)
        return run_pipeline(bronze_claims, bronze_members)

    def test_monthly_kpi_matches_golden(self, spark, golden_data_dir, outputs):
        actual   = outputs["monthly_kpi"]
        expected = _expected_monthly_df(spark, golden_data_dir)

        assert_df_equality(
            actual,
            expected,
            ignore_row_order=True,
            ignore_column_order=True,
            ignore_nullable=True,   # aggregates produce nullable cols
        )

    def test_member_kpi_matches_golden(self, spark, golden_data_dir, outputs):
        actual   = outputs["member_kpi"]
        expected = _expected_member_df(spark, golden_data_dir)

        assert_df_equality(
            actual,
            expected,
            ignore_row_order=True,
            ignore_column_order=True,
            ignore_nullable=True,
        )

    def test_silver_row_count_stable(self, outputs):
        """
        Row-count canary: golden inputs have 8 valid claims, so silver should
        always have 8 rows. A change here means either logic moved or inputs did.
        """
        assert outputs["silver_claims"].count() == 8

    def test_silver_schema_stable(self, outputs):
        """Catches accidentally adding/removing/renaming silver columns."""
        expected_cols = {
            "claim_id", "member_id", "provider_id", "claim_date",
            "claim_amount", "status", "denial_code", "is_denied",
            "claim_amount_band", "plan_type", "state",
            "_ingestion_timestamp",
        }
        assert set(outputs["silver_claims"].columns) == expected_cols


# ---------------------------------------------------------------------------
# Golden regeneration helper
# Run with: `python -m tests.regression.test_end_to_end`
# Reviews the diff before committing.
# ---------------------------------------------------------------------------
def _regenerate_goldens():
    """Recompute and overwrite the golden expected_*.json files."""
    here = Path(__file__).parent / "golden_data"
    spark = SparkSession.builder.master("local[2]").appName("regen").getOrCreate()

    bronze_claims  = _bronze_claims_from_golden(spark, here)
    bronze_members = _bronze_members_from_golden(spark, here)
    out = run_pipeline(bronze_claims, bronze_members)

    monthly = [
        {
            "claim_year_month":   r["claim_year_month"],
            "status":             r["status"],
            "claim_count":        int(r["claim_count"]),
            "total_claim_amount": str(r["total_claim_amount"]),
            "avg_claim_amount":   str(r["avg_claim_amount"]),
        }
        for r in out["monthly_kpi"].orderBy("claim_year_month", "status").collect()
    ]
    (here / "expected_monthly_kpi.json").write_text(json.dumps(monthly, indent=2))

    member = [
        {
            "member_id":           r["member_id"],
            "total_claims":        int(r["total_claims"]),
            "total_paid_amount":   str(r["total_paid_amount"]),
            "total_denied_claims": int(r["total_denied_claims"]),
            "first_claim_date":    r["first_claim_date"].isoformat(),
            "last_claim_date":     r["last_claim_date"].isoformat(),
        }
        for r in out["member_kpi"].orderBy("member_id").collect()
    ]
    (here / "expected_member_kpi.json").write_text(json.dumps(member, indent=2))

    print("Regenerated golden files. REVIEW THE DIFF BEFORE COMMITTING.")
    spark.stop()


if __name__ == "__main__":
    _regenerate_goldens()
