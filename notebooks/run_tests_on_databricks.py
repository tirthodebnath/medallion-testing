# Databricks notebook source
# MAGIC %md
# MAGIC # Run pytest on Databricks (Free Edition / serverless)
# MAGIC
# MAGIC Use this notebook to execute the test suite natively on Databricks
# MAGIC serverless compute.
# MAGIC
# MAGIC Two ways to run this:
# MAGIC
# MAGIC 1. **Interactively** — open this notebook from your Git folder, click
# MAGIC    **Connect → Serverless** in the top right, then Run All.
# MAGIC 2. **As a job** — `databricks bundle deploy` then `databricks bundle
# MAGIC    run medallion_test_job` will execute this notebook serverlessly.

# COMMAND ----------

# MAGIC %pip install pytest==8.3.3 chispa==0.10.1

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Copy project to a writable location
# MAGIC
# MAGIC Workspace Git folders are mounted read-only. Python needs to write
# MAGIC `__pycache__/` when importing, and pytest writes `.pytest_cache/`.
# MAGIC Copying the project to `/tmp/` gives us a normal writable filesystem
# MAGIC where everything works without workarounds.

# COMMAND ----------

import os, sys, shutil

NOTEBOOK_PATH = (
    dbutils.notebook.entry_point.getDbutils()
           .notebook().getContext().notebookPath().get()
)
REPO_PATH = "/Workspace" + os.path.dirname(NOTEBOOK_PATH).rsplit("/notebooks", 1)[0]

WORK_DIR = "/tmp/medallion-testing"
if os.path.exists(WORK_DIR):
    shutil.rmtree(WORK_DIR)
shutil.copytree(REPO_PATH, WORK_DIR)

sys.path.insert(0, WORK_DIR)
os.chdir(WORK_DIR)

print(f"Source:   {REPO_PATH}")
print(f"Work dir: {WORK_DIR}")
print(f"cwd:      {os.getcwd()}")

# Sanity check: src package is importable
import src.common.schemas
print(f"src found: {src.common.schemas.__file__}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run pytest

# COMMAND ----------

import pytest

exit_code = pytest.main([
    "tests",
    "-m", "not integration",
    "-ra",
    "--tb=short",
])

print(f"\npytest exit code: {exit_code}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fail the job if any test failed

# COMMAND ----------

assert exit_code == 0, f"Tests failed (pytest exit code {exit_code})"
