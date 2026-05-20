"""
Silver layer: cleanse, type-cast, dedupe, enrich, conform.

Inputs : Bronze claims + Bronze members.
Output : Silver claims conforming to CLAIMS_SILVER_SCHEMA.

Each step is its own function so tests can attack them in isolation.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.utils import (
    classify_claim_amount,
    is_denied_flag,
    dedupe_latest,
)


# ---------------------------------------------------------------------------
# Individual transformation steps (pure, easy to unit-test)
# ---------------------------------------------------------------------------
def cast_claims_types(df: DataFrame) -> DataFrame:
    """Cast string columns from Bronze into proper Silver types."""
    return (
        df.withColumn("claim_date",   F.to_date("claim_date", "yyyy-MM-dd"))
          .withColumn("claim_amount", F.col("claim_amount").cast("decimal(12,2)"))
    )


def filter_invalid_claims(df: DataFrame) -> DataFrame:
    """
    Drop records that violate non-negotiable invariants:
      - claim_id is null
      - member_id is null
      - claim_date is null (failed to parse)
      - claim_amount is null or negative
    """
    return df.filter(
        F.col("claim_id").isNotNull()
        & F.col("member_id").isNotNull()
        & F.col("claim_date").isNotNull()
        & F.col("claim_amount").isNotNull()
        & (F.col("claim_amount") >= 0)
    )


def enrich_with_business_fields(df: DataFrame) -> DataFrame:
    """Add derived columns: is_denied flag and claim_amount_band."""
    return (
        df.withColumn("is_denied",         is_denied_flag(F.col("status")))
          .withColumn("claim_amount_band", classify_claim_amount(F.col("claim_amount")))
    )


def join_members(claims_df: DataFrame, members_df: DataFrame) -> DataFrame:
    """
    LEFT JOIN claims to members on member_id to bring plan_type + state.

    LEFT (not inner) so we never silently drop claims whose member is missing
    from the reference table — those will surface in DQ checks instead.
    """
    members_slim = members_df.select("member_id", "plan_type", "state")
    return claims_df.join(members_slim, on="member_id", how="left")


# ---------------------------------------------------------------------------
# Composed entry point
# ---------------------------------------------------------------------------
def build_silver_claims(
    bronze_claims: DataFrame,
    bronze_members: DataFrame,
) -> DataFrame:
    """
    Full Bronze -> Silver pipeline for claims.

    Steps:
      1. Cast types
      2. Filter invalid records
      3. Enrich with business fields
      4. Join member reference data
      5. Dedupe by claim_id (latest by ingestion timestamp)
      6. Project to the Silver contract
    """
    typed   = cast_claims_types(bronze_claims)
    valid   = filter_invalid_claims(typed)
    enriched = enrich_with_business_fields(valid)
    joined  = join_members(enriched, bronze_members)
    deduped = dedupe_latest(joined, key_cols=["claim_id"], order_col="_ingestion_timestamp")

    return deduped.select(
        "claim_id",
        "member_id",
        "provider_id",
        "claim_date",
        "claim_amount",
        "status",
        "denial_code",
        "is_denied",
        "claim_amount_band",
        "plan_type",
        "state",
        "_ingestion_timestamp",
    )
