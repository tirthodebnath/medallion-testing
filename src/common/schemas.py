"""
Centralized schema definitions for the Medallion pipeline.

Keeping schemas explicit (rather than inferred) makes the pipeline robust to
upstream changes and makes test fixtures cheaper to build.
"""
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DecimalType, DateType, TimestampType, BooleanType
)

# ---------------------------------------------------------------------------
# Bronze - raw landing schemas (everything string except already-typed audit cols)
# ---------------------------------------------------------------------------
CLAIMS_RAW_SCHEMA = StructType([
    StructField("claim_id",      StringType(), True),
    StructField("member_id",     StringType(), True),
    StructField("provider_id",   StringType(), True),
    StructField("claim_date",    StringType(), True),
    StructField("claim_amount",  StringType(), True),
    StructField("status",        StringType(), True),
    StructField("denial_code",   StringType(), True),
])

MEMBERS_RAW_SCHEMA = StructType([
    StructField("member_id",        StringType(), True),
    StructField("member_name",      StringType(), True),
    StructField("plan_type",        StringType(), True),
    StructField("enrollment_date",  StringType(), True),
    StructField("state",            StringType(), True),
])

# ---------------------------------------------------------------------------
# Silver - cleansed, typed, conformed
# ---------------------------------------------------------------------------
CLAIMS_SILVER_SCHEMA = StructType([
    StructField("claim_id",        StringType(),     False),
    StructField("member_id",       StringType(),     False),
    StructField("provider_id",     StringType(),     True),
    StructField("claim_date",      DateType(),       False),
    StructField("claim_amount",    DecimalType(12, 2), False),
    StructField("status",          StringType(),     False),
    StructField("denial_code",     StringType(),     True),
    StructField("is_denied",       BooleanType(),    False),
    StructField("claim_amount_band", StringType(),   False),
    StructField("plan_type",       StringType(),     True),
    StructField("state",           StringType(),     True),
    StructField("_ingestion_timestamp", TimestampType(), False),
])

# ---------------------------------------------------------------------------
# Gold - business-ready aggregates
# ---------------------------------------------------------------------------
MONTHLY_CLAIMS_KPI_SCHEMA = StructType([
    StructField("claim_year_month", StringType(),     False),   # YYYY-MM
    StructField("status",           StringType(),     False),
    StructField("claim_count",      IntegerType(),    False),
    StructField("total_claim_amount", DecimalType(18, 2), False),
    StructField("avg_claim_amount",   DecimalType(18, 4), False),
])

MEMBER_LIFETIME_KPI_SCHEMA = StructType([
    StructField("member_id",            StringType(),    False),
    StructField("total_claims",         IntegerType(),   False),
    StructField("total_paid_amount",    DecimalType(18, 2), False),
    StructField("total_denied_claims", IntegerType(),    False),
    StructField("first_claim_date",     DateType(),      True),
    StructField("last_claim_date",      DateType(),      True),
])
