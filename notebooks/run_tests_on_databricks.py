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

# MAGIC %md
# MAGIC ## 1. Install test dependencies
# MAGIC
# MAGIC `%pip install` works on serverless via Databricks' internal PyPI proxy,
# MAGIC so this works even though Free Edition restricts general outbound
# MAGIC internet access.

# COMMAND ----------

# MAGIC %pip install pytest==8.3.3 chispa==0.10.1
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Locate the repo
# MAGIC
# MAGIC When this notebook is opened via a Databricks Git folder, the repo root
# MAGIC is one level above `notebooks/`. The block below derives that path,
# MAGIC adds `src/` to `sys.path` so the package imports work, and changes
# MAGIC the working directory to the repo root so pytest's discovery picks
# MAGIC up `pytest.ini` and the `tests/` tree.

# COMMAND ----------

import os
import sys

# Workspace files are read-only — Python cannot create __pycache__ dirs.
sys.dont_write_bytecode = True
os.environ["PYTHONDONTWRITEBYTECODE"] = "1"

NOTEBOOK_PATH = (
    dbutils.notebook.entry_point.getDbutils()
           .notebook().getContext().notebookPath().get()
)
# /Users/<user>/medallion-testing/notebooks/run_tests_on_databricks
#  ->  /Workspace/Users/<user>/medallion-testing
REPO_PATH = "/Workspace" + os.path.dirname(NOTEBOOK_PATH).rsplit("/notebooks", 1)[0]

sys.path.insert(0, REPO_PATH)
os.chdir(REPO_PATH)

print(f"REPO_PATH    = {REPO_PATH}")
print(f"cwd          = {os.getcwd()}")
print(f"sys.path[0]  = {sys.path[0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Run pytest
# MAGIC
# MAGIC On Free Edition serverless we skip the integration tests because they
# MAGIC need to write Delta files to a path serverless executors can persist
# MAGIC to — Free Edition's storage surface is limited. Unit, regression, and
# MAGIC data quality tests run fine.

# COMMAND ----------

import pytest

exit_code = pytest.main([
    "tests",
    "-m", "not integration",
    "-ra",
    "--tb=short",
    "--override-ini=cache_dir=/tmp/.pytest_cache",
])

print(f"\npytest exit code: {exit_code}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Fail the job if any test failed
# MAGIC
# MAGIC `exit_code` follows pytest conventions: 0 = all passed, non-zero =
# MAGIC failures. Raising here propagates to the Databricks Job UI as a
# MAGIC failed task.

# COMMAND ----------

assert exit_code == 0, f"Tests failed (pytest exit code {exit_code})"
