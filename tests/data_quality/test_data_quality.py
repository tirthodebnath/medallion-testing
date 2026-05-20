"""
Data Quality (DQ) assertions on pipeline outputs.

These are conceptually different from unit tests:
  - Unit tests verify that code does what we wrote.
  - DQ tests verify that the *output* satisfies business invariants
    regardless of how the code was implemented.

In production, the same checks would also run as `expectations` against the
live Delta tables (e.g., via DLT expectations or Great Expectations). The
test-suite form here protects PR-time regressions.
"""
import pytest
from pyspark.sql import functions as F

from src.pipeline import run_pipeline


@pytest.fixture
def pipeline_outputs(bronze_claims_df, bronze_members_df):
    return run_pipeline(bronze_claims_df, bronze_members_df)


# ---------------------------------------------------------------------------
# Silver invariants
# ---------------------------------------------------------------------------
class TestSilverDataQuality:

    def test_claim_id_is_unique(self, pipeline_outputs):
        silver = pipeline_outputs["silver_claims"]
        total  = silver.count()
        distinct = silver.select("claim_id").distinct().count()
        assert total == distinct, "claim_id must be unique in silver"

    def test_no_null_critical_columns(self, pipeline_outputs):
        silver = pipeline_outputs["silver_claims"]
        critical = ["claim_id", "member_id", "claim_date", "claim_amount", "status"]
        nulls = silver.filter(
            " OR ".join(f"{c} IS NULL" for c in critical)
        ).count()
        assert nulls == 0, "no critical column may be null in silver"

    def test_claim_amount_non_negative(self, pipeline_outputs):
        silver = pipeline_outputs["silver_claims"]
        assert silver.filter("claim_amount < 0").count() == 0

    def test_claim_amount_band_in_allowed_values(self, pipeline_outputs):
        silver = pipeline_outputs["silver_claims"]
        bad = silver.filter(
            ~F.col("claim_amount_band").isin("LOW", "MEDIUM", "HIGH", "JUMBO", "UNKNOWN")
        ).count()
        assert bad == 0

    def test_is_denied_flag_consistent_with_status(self, pipeline_outputs):
        """is_denied=True iff status='DENIED' (case-insensitive)."""
        silver = pipeline_outputs["silver_claims"]
        inconsistent = silver.filter(
            (F.upper("status") == "DENIED") != F.col("is_denied")
        ).count()
        assert inconsistent == 0


# ---------------------------------------------------------------------------
# Gold invariants
# ---------------------------------------------------------------------------
class TestGoldDataQuality:

    def test_monthly_kpi_one_row_per_month_status(self, pipeline_outputs):
        gold = pipeline_outputs["monthly_kpi"]
        total = gold.count()
        distinct = gold.select("claim_year_month", "status").distinct().count()
        assert total == distinct

    def test_member_kpi_one_row_per_member(self, pipeline_outputs):
        gold = pipeline_outputs["member_kpi"]
        total = gold.count()
        distinct = gold.select("member_id").distinct().count()
        assert total == distinct

    def test_member_paid_plus_denied_le_total(self, pipeline_outputs):
        """
        total_paid_amount is the sum of non-denied amounts. We can't directly
        assert it equals (total - denied_amount) from this table alone, but
        we can assert total_denied_claims <= total_claims.
        """
        gold = pipeline_outputs["member_kpi"]
        bad = gold.filter("total_denied_claims > total_claims").count()
        assert bad == 0

    def test_total_paid_is_non_negative(self, pipeline_outputs):
        gold = pipeline_outputs["member_kpi"]
        assert gold.filter("total_paid_amount < 0").count() == 0

    def test_first_claim_le_last_claim(self, pipeline_outputs):
        gold = pipeline_outputs["member_kpi"]
        bad = gold.filter("first_claim_date > last_claim_date").count()
        assert bad == 0


# ---------------------------------------------------------------------------
# Cross-layer reconciliation
# ---------------------------------------------------------------------------
class TestCrossLayerReconciliation:
    """
    Independent recomputation: take silver, reaggregate in the test, and
    confirm gold matches. If gold has a bug, silver-derived totals will
    not match gold totals.
    """

    def test_monthly_kpi_totals_reconcile_against_silver(self, pipeline_outputs):
        silver = pipeline_outputs["silver_claims"]
        gold   = pipeline_outputs["monthly_kpi"]

        silver_total = silver.agg(F.sum("claim_amount").alias("t")).first()["t"]
        gold_total   = gold.agg(F.sum("total_claim_amount").alias("t")).first()["t"]

        assert silver_total == gold_total, \
            f"silver total {silver_total} != gold total {gold_total}"

    def test_member_kpi_claim_count_reconciles(self, pipeline_outputs):
        silver = pipeline_outputs["silver_claims"]
        gold   = pipeline_outputs["member_kpi"]

        silver_count = silver.count()
        gold_count   = gold.agg(F.sum("total_claims").alias("t")).first()["t"]
        assert silver_count == gold_count
