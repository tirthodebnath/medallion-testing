# Databricks notebook source
# MAGIC %md
# MAGIC # Medallion Pipeline — Bronze → Silver → Gold
# MAGIC
# MAGIC Reads raw CSVs from the landing Volume, runs the full pipeline,
# MAGIC and writes results to Unity Catalog Delta tables.
# MAGIC
# MAGIC **Before running:** upload `claims.csv` and `members.csv` to:
# MAGIC ```
# MAGIC /Volumes/workspace/tirtho_db/tirtho_uploaded_files/claims.csv
# MAGIC /Volumes/workspace/tirtho_db/tirtho_uploaded_files/members.csv
# MAGIC ```

# COMMAND ----------

# MAGIC %pip install chispa==0.10.1

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG = "workspace"
SCHEMA  = "tirtho_db"
VOLUME  = "tirtho_uploaded_files"

CLAIMS_LANDING  = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/claims.csv"
MEMBERS_LANDING = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/members.csv"

BRONZE_CLAIMS_TABLE  = f"{CATALOG}.{SCHEMA}.bronze_claims"
BRONZE_MEMBERS_TABLE = f"{CATALOG}.{SCHEMA}.bronze_members"
SILVER_CLAIMS_TABLE  = f"{CATALOG}.{SCHEMA}.silver_claims"
GOLD_MONTHLY_TABLE   = f"{CATALOG}.{SCHEMA}.gold_monthly_claims_kpi"
GOLD_MEMBER_TABLE    = f"{CATALOG}.{SCHEMA}.gold_member_lifetime_kpi"

print(f"Claims landing:  {CLAIMS_LANDING}")
print(f"Members landing: {MEMBERS_LANDING}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up imports

# COMMAND ----------

import os, sys, shutil

NOTEBOOK_PATH = (
    dbutils.notebook.entry_point.getDbutils()
           .notebook().getContext().notebookPath().get()
)
REPO_PATH = "/Workspace" + os.path.dirname(NOTEBOOK_PATH).rsplit("/notebooks", 1)[0]

# Copy to /tmp so imports work cleanly (Workspace files are read-only)
WORK_DIR = "/tmp/medallion-pipeline"
if os.path.exists(WORK_DIR):
    shutil.rmtree(WORK_DIR)
shutil.copytree(REPO_PATH, WORK_DIR)
sys.path.insert(0, WORK_DIR)

sys.dont_write_bytecode = True

# COMMAND ----------

from pyspark.sql import SparkSession
from src.bronze.ingest import ingest_claims, ingest_members
from src.silver.transform import build_silver_claims
from src.gold.aggregate import build_monthly_claims_kpi, build_member_lifetime_kpi

spark = SparkSession.builder.getOrCreate()
print("Imports OK")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze — ingest raw data

# COMMAND ----------

bronze_claims = ingest_claims(spark, CLAIMS_LANDING, source_format="csv")
bronze_members = ingest_members(spark, MEMBERS_LANDING, source_format="csv")

print(f"Bronze claims:  {bronze_claims.count()} rows")
print(f"Bronze members: {bronze_members.count()} rows")

bronze_claims.write.format("delta").mode("overwrite").saveAsTable(BRONZE_CLAIMS_TABLE)
bronze_members.write.format("delta").mode("overwrite").saveAsTable(BRONZE_MEMBERS_TABLE)
print("Bronze tables written.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver — cleanse, dedupe, enrich

# COMMAND ----------

bc = spark.table(BRONZE_CLAIMS_TABLE)
bm = spark.table(BRONZE_MEMBERS_TABLE)

silver_claims = build_silver_claims(bc, bm)
print(f"Silver claims: {silver_claims.count()} rows")

silver_claims.write.format("delta").mode("overwrite").saveAsTable(SILVER_CLAIMS_TABLE)
print("Silver table written.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold — business KPIs

# COMMAND ----------

sc = spark.table(SILVER_CLAIMS_TABLE)

monthly_kpi = build_monthly_claims_kpi(sc)
member_kpi  = build_member_lifetime_kpi(sc)

print(f"Monthly KPI: {monthly_kpi.count()} rows")
print(f"Member KPI:  {member_kpi.count()} rows")

monthly_kpi.write.format("delta").mode("overwrite").saveAsTable(GOLD_MONTHLY_TABLE)
member_kpi.write.format("delta").mode("overwrite").saveAsTable(GOLD_MEMBER_TABLE)
print("Gold tables written.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

print("=== Monthly Claims KPI ===")
spark.table(GOLD_MONTHLY_TABLE).show(truncate=False)

print("=== Member Lifetime KPI ===")
spark.table(GOLD_MEMBER_TABLE).show(truncate=False)

print("Pipeline complete.")
