"""
Gold layer: business-ready aggregates consumed by BI / downstream.

Each KPI is computed by a small, individually-testable function.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_monthly_claims_kpi(silver_claims: DataFrame) -> DataFrame:
    """
    Monthly aggregate of claim counts and amounts by status.

    Columns:
      claim_year_month  (YYYY-MM)
      status
      claim_count
      total_claim_amount
      avg_claim_amount
    """
    return (
        silver_claims
        .withColumn("claim_year_month", F.date_format("claim_date", "yyyy-MM"))
        .groupBy("claim_year_month", "status")
        .agg(
            F.count("*").cast("int").alias("claim_count"),
            F.sum("claim_amount").cast("decimal(18,2)").alias("total_claim_amount"),
            F.avg("claim_amount").cast("decimal(18,4)").alias("avg_claim_amount"),
        )
        .orderBy("claim_year_month", "status")
    )


def build_member_lifetime_kpi(silver_claims: DataFrame) -> DataFrame:
    """
    Lifetime claims summary per member.

    'total_paid_amount' counts amount only for non-denied claims
    (denied claims do not contribute to paid).
    """
    paid_amount = F.when(~F.col("is_denied"), F.col("claim_amount")).otherwise(F.lit(0))
    denied_flag = F.when(F.col("is_denied"), F.lit(1)).otherwise(F.lit(0))

    return (
        silver_claims
        .groupBy("member_id")
        .agg(
            F.count("*").cast("int").alias("total_claims"),
            F.sum(paid_amount).cast("decimal(18,2)").alias("total_paid_amount"),
            F.sum(denied_flag).cast("int").alias("total_denied_claims"),
            F.min("claim_date").alias("first_claim_date"),
            F.max("claim_date").alias("last_claim_date"),
        )
        .orderBy("member_id")
    )
