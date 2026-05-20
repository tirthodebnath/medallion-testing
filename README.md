# Medallion Testing — End-to-End Example for Databricks

A working reference implementation of a Bronze → Silver → Gold pipeline
with **unit, integration, regression, and data-quality** tests, designed
to run natively on Databricks (and locally via pytest for fast feedback).

The domain is healthcare claims, deliberately small but realistic — three
member dimensions, eight claims, two gold KPIs.

---

## TL;DR

```bash
# Local
pip install -r requirements-dev.txt
pytest tests -m "not integration"     # 59 tests, ~30s on a laptop
pytest tests                          # full suite incl. Delta integration

# Databricks
databricks bundle deploy
databricks bundle run medallion_test_job
```

---

## Project layout

```
medallion_testing/
├── src/
│   ├── common/
│   │   ├── schemas.py      # Bronze/Silver/Gold schemas (single source of truth)
│   │   └── utils.py        # Pure helpers: audit cols, banding, dedupe
│   ├── bronze/ingest.py    # Raw landing + audit columns
│   ├── silver/transform.py # Cast, filter invalids, enrich, join, dedupe
│   ├── gold/aggregate.py   # Monthly KPI + member-lifetime KPI
│   └── pipeline.py         # run_pipeline() (pure) + main() (job entry point)
├── tests/
│   ├── conftest.py         # SparkSession fixtures (plain + Delta), row builders
│   ├── unit/               # ~40 tests on individual functions
│   ├── integration/        # Delta round-trip Bronze→Silver→Gold
│   ├── regression/         # End-to-end vs. golden datasets
│   │   └── golden_data/    # Fixed input + expected output JSONs
│   └── data_quality/       # Invariants that must hold on outputs
├── notebooks/
│   └── run_tests_on_databricks.py  # Run pytest natively on a cluster
├── databricks.yml          # Asset Bundle (pipeline job + test job)
├── azure-pipelines.yml     # CI: pytest on PR, deploy on merge
├── pytest.ini
├── requirements.txt
└── requirements-dev.txt
```

---

## The four test categories — what they protect against

| Layer            | What it asserts                                              | Speed |
|------------------|--------------------------------------------------------------|-------|
| **Unit**         | Individual functions return correct outputs on small inputs  | Fast  |
| **Integration**  | Layers compose correctly through real Delta IO               | Slow  |
| **Regression**   | End-to-end output is byte-identical to a fixed golden output | Med   |
| **Data quality** | Output satisfies business invariants (uniqueness, ranges…)   | Med   |

Why separate regression and DQ? They guard different failure modes:
regression catches *intended-but-unreviewed* logic changes (output shifted);
DQ catches *implementation bugs that pass the goldens but violate
invariants* (e.g., a logic change that happens to match the golden for the
golden input but breaks uniqueness on real data).

---

## Pipeline contract

### Bronze
- Read CSV/JSON verbatim with explicit string schema.
- Attach `_ingestion_timestamp`, `_source_file`.
- Append to Delta.

### Silver
1. Cast `claim_date` → `date`, `claim_amount` → `decimal(12,2)`.
2. Drop records with null `claim_id`/`member_id`/`claim_date`/`claim_amount`
   or negative amounts. (Zero is allowed — adjusted claims.)
3. Add derived: `is_denied`, `claim_amount_band` (LOW / MEDIUM / HIGH / JUMBO).
4. LEFT JOIN `members` to bring `plan_type`, `state`. (LEFT, not INNER,
   so orphan claims surface as DQ failures rather than disappear.)
5. Dedupe by `claim_id` keeping the latest `_ingestion_timestamp`.

### Gold
- `monthly_claims_kpi` — claim_count, total/avg amount by month × status.
- `member_lifetime_kpi` — counts, paid amount (excludes denied), denied
  count, first/last claim date per member.

---

## Running tests

### Locally (recommended during development)

```bash
pytest tests -m "not integration"   # fast: unit + regression + DQ
pytest tests/unit                   # one layer
pytest tests -v                     # verbose
pytest tests --cov=src              # with coverage
```

`pytest.ini` declares the `integration` marker so you can scope runs.

### Locally with Delta (integration tests)

Integration tests use the `spark_delta` fixture, which calls
`configure_spark_with_delta_pip()` to pull the right Delta JARs from Maven
on first use. First run takes ~30 s extra.

```bash
pytest tests/integration
```

### On a Databricks cluster

Open `notebooks/run_tests_on_databricks.py` from your Repo, attach to a
cluster, and run all cells. Or schedule it as a job — the deployed
`medallion_test_job` (see `databricks.yml`) does exactly this.

---

## Regression workflow — when expected outputs *should* change

You changed the logic on purpose. The regression test will fail. Do not
hand-edit the goldens; instead:

```bash
python -m tests.regression.test_end_to_end   # regenerates expected_*.json
git diff tests/regression/golden_data/        # **review carefully**
git add tests/regression/golden_data/
```

The PR diff on the golden file is the human-review checkpoint that prevents
silent output changes.

---

## Notes on the conftest patterns

A few things in `tests/conftest.py` are worth calling out — they’re the
moves that keep this suite cheap to run and easy to extend:

1. **Session-scoped SparkSession.** Spinning up Spark is the slowest thing
   in the suite. One per session, reused everywhere.
2. **Two Spark fixtures — `spark` (plain) and `spark_delta`.** Unit tests
   don’t need Delta JARs on the classpath; integration tests do. Splitting
   them keeps the fast path fast and avoids classpath surprises.
3. **Explicit schemas in row builders.** Spark can’t infer types from rows
   where a column is all-None — common in fixtures. Declaring the schema
   once removes a whole category of flaky tests.
4. **Row builders, not row constants.** Tests can tweak one field
   (`make_bronze_claim(claim_amount="-50.00")`) without re-declaring the
   other eight, keeping intent in the test.

---

## CI / CD

`azure-pipelines.yml` runs `pytest -m "not integration"` on every PR on
the build agent, then on merge to `main` deploys the DAB to your workspace
and triggers `medallion_test_job` on Databricks (which runs the full suite,
integration included, against real Delta).

Adapt to GitHub Actions / GitLab CI by replacing the deploy stage; the
pytest invocation is the same.

> Before the bundle deploys, replace the `host:` placeholder in
> `databricks.yml` with your real workspace URL (e.g.
> `https://adb-1234567890123456.7.azuredatabricks.net`).

---

## Mocking `dbutils`

Bronze/Silver/Gold code in `src/` does **not** import `dbutils` — that’s
deliberate. `dbutils` belongs at the orchestration edge (notebooks, job
entry points), not in transformation code. If you need it in a test,
the simplest pattern is:

```python
class _StubDbutils:
    class fs:
        @staticmethod
        def ls(_): return []

def test_something_that_takes_dbutils(monkeypatch):
    monkeypatch.setitem(__builtins__, "dbutils", _StubDbutils())
    ...
```

But the better move is to refactor: take a list of files as an argument
instead of reading `dbutils.fs.ls` inside the transform.
