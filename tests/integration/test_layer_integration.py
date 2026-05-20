"""
Integration tests: wire layers together using real Delta tables on disk.

These are slower than unit tests but catch issues that pure-DataFrame tests
miss — e.g., partitioning, write-then-read schema drift, type stability
through Delta serialization.

Marked with @pytest.mark.integration so you can run unit tests fast
with `pytest -m "not integration"` during local development.
"""
from datetime import date
from decimal import Decimal

import pytest

from src.bronze.ingest import ingest_claims, ingest_members
from src.silver.transform import build_silver_claims
from src.gold.aggregate import build_monthly_claims_kpi


pytestmark = pytest.mark.integration


def _write_csv(path, header, rows):
    path.write_text(header + "\n" + "\n".join(rows) + "\n")


class TestBronzeToSilverWithDelta:

    def test_round_trip_through_delta(self, spark_delta, tmp_path, tmp_warehouse):
        spark = spark_delta  # local alias keeps body readable
        # 1. Land a CSV
        claims_csv = tmp_path / "claims.csv"
        _write_csv(
            claims_csv,
            "claim_id,member_id,provider_id,claim_date,claim_amount,status,denial_code",
            [
                "C001,M001,P001,2024-01-15,250.00,PAID,",
                "C002,M001,P002,2024-01-20,1500.00,DENIED,D01",
            ],
        )
        members_csv = tmp_path / "members.csv"
        _write_csv(
            members_csv,
            "member_id,member_name,plan_type,enrollment_date,state",
            ["M001,Alice,GOLD,2022-01-01,NY"],
        )

        # 2. Ingest to Bronze DataFrame and persist as Delta
        bronze_claims  = ingest_claims(spark, str(claims_csv))
        bronze_members = ingest_members(spark, str(members_csv))

        bronze_claims_path  = str(tmp_warehouse / "bronze_claims")
        bronze_members_path = str(tmp_warehouse / "bronze_members")
        bronze_claims.write.format("delta").mode("overwrite").save(bronze_claims_path)
        bronze_members.write.format("delta").mode("overwrite").save(bronze_members_path)

        # 3. Read Bronze back and build Silver
        bc = spark.read.format("delta").load(bronze_claims_path)
        bm = spark.read.format("delta").load(bronze_members_path)
        silver = build_silver_claims(bc, bm)

        # 4. Persist Silver and read back -- types must survive Delta round-trip
        silver_path = str(tmp_warehouse / "silver_claims")
        silver.write.format("delta").mode("overwrite").save(silver_path)
        silver_back = spark.read.format("delta").load(silver_path)

        # Assertions
        assert silver_back.count() == 2
        row = silver_back.filter("claim_id = 'C001'").first()
        assert row["claim_amount"] == Decimal("250.00")
        assert row["claim_date"] == date(2024, 1, 15)
        assert row["plan_type"] == "GOLD"
        assert row["is_denied"] is False


class TestSilverToGoldWithDelta:

    def test_monthly_kpi_from_persisted_silver(self, spark_delta, tmp_warehouse, bronze_claims_df, bronze_members_df):
        spark = spark_delta
        silver = build_silver_claims(bronze_claims_df, bronze_members_df)
        silver_path = str(tmp_warehouse / "silver_claims")
        silver.write.format("delta").mode("overwrite").save(silver_path)

        silver_back = spark.read.format("delta").load(silver_path)
        monthly = build_monthly_claims_kpi(silver_back)

        # Fixture has: Jan PAID(C001), Jan DENIED(C002), Feb PAID(C003), Feb PAID(C004)
        result = {(r["claim_year_month"], r["status"]): r["claim_count"]
                  for r in monthly.collect()}
        assert result[("2024-01", "PAID")]   == 1
        assert result[("2024-01", "DENIED")] == 1
        assert result[("2024-02", "PAID")]   == 2

    def test_idempotent_overwrite(self, spark_delta, tmp_warehouse, bronze_claims_df, bronze_members_df):
        """
        Running the silver build twice and overwriting the table must
        produce the same row count -- catches accidental append-mode bugs.
        """
        spark = spark_delta
        silver = build_silver_claims(bronze_claims_df, bronze_members_df)
        path = str(tmp_warehouse / "silver_claims")

        for _ in range(2):
            silver.write.format("delta").mode("overwrite").save(path)

        assert spark.read.format("delta").load(path).count() == 4
