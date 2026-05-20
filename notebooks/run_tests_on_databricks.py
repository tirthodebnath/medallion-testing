# Databricks notebook source
# MAGIC %md
# MAGIC # Run pytest on the Databricks cluster
# MAGIC
# MAGIC Use this notebook to execute the test suite natively on a Databricks
# MAGIC cluster (useful for catching environment-specific issues that local
# MAGIC pytest may miss — e.g., Unity Catalog access, cluster lib versions).
# MAGIC
# MAGIC Two ways to run this:
# MAGIC
# MAGIC 1. **Interactively** — attach to a cluster and run all cells.
# MAGIC 2. **As a job** — schedule this notebook in a Databricks Job (or
# MAGIC    Asset Bundle), failing the job if any test fails.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Install test dependencies on the cluster
# MAGIC
# MAGIC `%pip install` makes the libs available to the notebook's Python interpreter.
# MAGIC For production jobs, list these in `databricks.yml` under `libraries`
# MAGIC so the cluster has them at startup instead of installing per-run.

# COMMAND ----------

# MAGIC %pip install pytest==8.3.3 chispa==0.10.1 pytest-html==4.1.1
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Locate the repo
# MAGIC
# MAGIC When this notebook is opened via Databricks Repos / Git Folders, its
# MAGIC parent directory is the repo root. Adjust `REPO_PATH` if you placed
# MAGIC the repo elsewhere (e.g. workspace path / mounted volume).

# COMMAND ----------

import os
import sys

# Default: the repo root is one level above this notebook.
NOTEBOOK_PATH = (
    dbutils.notebook.entry_point.getDbutils()
           .notebook().getContext().notebookPath().get()
)
REPO_PATH = "/Workspace" + os.path.dirname(NOTEBOOK_PATH).rsplit("/notebooks", 1)[0]

# Make src importable
sys.path.insert(0, REPO_PATH)
os.chdir(REPO_PATH)
print(f"REPO_PATH = {REPO_PATH}")
print(f"sys.path[0] = {sys.path[0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Run pytest

# COMMAND ----------

import pytest

# Pick the suite(s) you want:
#  - "tests/unit"          fast feedback
#  - "tests/integration"   needs Delta on the cluster
#  - "tests/regression"    end-to-end with golden datasets
#  - "tests/data_quality"  DQ invariants
#
# -ra : show short summary for non-passing tests
# --tb=short : compact tracebacks
# --html=...   pytest-html report stored in /dbfs for later download

REPORT_PATH = "/dbfs/tmp/pytest_report.html"

exit_code = pytest.main([
    "tests",
    "-ra",
    "--tb=short",
    f"--html={REPORT_PATH}",
    "--self-contained-html",
])

print(f"\npytest exit code: {exit_code}")
print(f"HTML report: {REPORT_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Fail the job if any test failed
# MAGIC
# MAGIC `exit_code` follows pytest conventions: 0 = all passed; non-zero = failures.
# MAGIC Raising here propagates to the Databricks Job UI as a failed task.

# COMMAND ----------

assert exit_code == 0, f"Tests failed (pytest exit code {exit_code}). See {REPORT_PATH}"
