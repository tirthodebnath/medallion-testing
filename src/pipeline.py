"""
End-to-end orchestration of the Bronze -> Silver -> Gold pipeline.

Designed so that:
  - It can be triggered as a Databricks Job entry point (main()).
  - The pure transformation function `run_pipeline()` accepts DataFrames
    and returns a dict of output DataFrames, which is what the regression
    test exercises end-to-end without needing real Delta tables.
"""
from typing import Dict

from pyspark.sql import DataFrame, SparkSession

from src.bronze.ingest import ingest_claims, ingest_members, write_bronze
from src.silver.transform import build_silver_claims
from src.gold.aggregate import (
    build_monthly_claims_kpi,
    build_member_lifetime_kpi,
)


def run_pipeline(
    bronze_claims: DataFrame,
    bronze_members: DataFrame,
) -> Dict[str, DataFrame]:
    """
    Pure function form of the pipeline — takes DataFrames in, returns
    a dict of named outputs. Trivial to call from a regression test.
    """
    silver_claims = build_silver_claims(bronze_claims, bronze_members)
    monthly_kpi   = build_monthly_claims_kpi(silver_claims)
    member_kpi    = build_member_lifetime_kpi(silver_claims)

    return {
        "silver_claims":   silver_claims,
        "monthly_kpi":     monthly_kpi,
        "member_kpi":      member_kpi,
    }


def main(
    claims_landing_path: str,
    members_landing_path: str,
    bronze_claims_table: str = "bronze.claims",
    bronze_members_table: str = "bronze.members",
    silver_claims_table: str = "silver.claims",
    gold_monthly_table:  str = "gold.monthly_claims_kpi",
    gold_member_table:   str = "gold.member_lifetime_kpi",
) -> None:
    """Job entry point — wires real reads and writes around `run_pipeline`."""
    spark = SparkSession.builder.getOrCreate()

    # Bronze: ingest + persist
    bronze_claims  = ingest_claims(spark, claims_landing_path)
    bronze_members = ingest_members(spark, members_landing_path)
    write_bronze(bronze_claims,  bronze_claims_table)
    write_bronze(bronze_members, bronze_members_table)

    # Silver + Gold built from Bronze tables we just wrote
    bronze_claims_tbl  = spark.table(bronze_claims_table)
    bronze_members_tbl = spark.table(bronze_members_table)
    outputs = run_pipeline(bronze_claims_tbl, bronze_members_tbl)

    outputs["silver_claims"].write.format("delta").mode("overwrite") \
        .saveAsTable(silver_claims_table)
    outputs["monthly_kpi"].write.format("delta").mode("overwrite") \
        .saveAsTable(gold_monthly_table)
    outputs["member_kpi"].write.format("delta").mode("overwrite") \
        .saveAsTable(gold_member_table)
