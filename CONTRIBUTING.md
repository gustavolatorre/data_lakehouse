# Contributing

## Local setup

Prerequisites: Docker Desktop, `uv` (Astral's package manager), `make`.

```bash
# 1. Clone + enter the repo
git clone https://github.com/gustavolatorre/data_lake.git
cd data_lake

# 2. Python deps (creates .venv)
uv venv --python 3.12
uv pip install -e ".[dev]"

# 3. Pre-commit hooks (lint + format + secrets scan on every commit)
make init-precommit

# 4. Generate Airflow secrets in airflow.env (Fernet, API key, JWT)
make init-secrets

# 5. Fill in MinIO / Postgres / Dremio credentials in .env (use strong values;
#    pydantic refuses banned defaults and anything < 12 chars)
cp .env.example .env  # then edit

# 6. Bring the stack up
make up
```

Open:
- Airflow UI: http://localhost:8080 (credentials = `${AIRFLOW_USER}` / `${AIRFLOW_PASSWORD}` from `.env`)
- MinIO console: http://localhost:9001
- Dremio UI: http://localhost:9047

## Workflow

1. **Branch from `main`** with a conventional prefix:
   - `feat/<thing>` — new functionality
   - `fix/<thing>` — bug or incident fix
   - `chore/<thing>` — tooling, deps, dev workflow
   - `docs/<thing>` — documentation only
2. Commit as you go. Pre-commit will format / lint / scan on every commit.
3. When ready, push and open a PR against `main`.
4. CI runs Lint, Test, Integration, dbt Validate, Security. The first four are
   blocking. In Security, the Trivy fs gate (fixable HIGH+CRITICAL) is also
   blocking (F-12); `pip-audit` and the Trivy config scan stay informational
   (`continue-on-error: true`).
5. After approval, squash-merge.

## Commit message conventions

Conventional commits (`type(scope): subject` + body):

```
feat(silver): add shrink guard before MERGE INTO

The MERGE's WHEN NOT MATCHED BY SOURCE deactivates rows globally, so a
partial fetch from the API would silently soft-delete half the table.

Adds _assert_source_not_shrinking which aborts the run when today's
source is more than 20% smaller than yesterday's is_active=true count.
```

Types we use: `feat`, `fix`, `chore`, `docs`, `refactor`, `test`, `ci`.

## Local quality gates (run before pushing)

```bash
make lint              # ruff check + ruff format --check + mypy
make test              # pytest + coverage (Java 17 required for Spark tests)
make security-scan     # mirrors the CI Security job's pip-audit run
```

Pre-commit will run a subset of these automatically.

## Adding a new DAG

1. Drop the file in `dags/`.
2. Import `from callbacks import build_failure_callback` and wire
   `on_failure_callback = build_failure_callback("YOUR LAYER")` into
   `default_args`. Don't redefine the callback function.
3. Use Asset URIs for reactive scheduling whenever possible.
4. Update `tests/unit/test_dags.py::EXPECTED_DAGS` so the AST validator
   covers your DAG too.

## Adding a Spark job

1. Drop the entry point under `src/{layer}/`.
2. Reuse `from src.utils.spark_session import create_spark_session` — never
   build the session inline.
3. Wrap the run in `try` / `finally spark.stop()`.
4. Use `cache()` + `unpersist()` around any count-driven validation chain;
   that's the same pattern in Bronze ingest and Silver transform.
5. Write unit tests with mocked `SparkSession` for the orchestration logic
   and tests against the `spark` fixture in `tests/conftest.py` for the
   actual transformations.

## Adding a dbt model

1. SQL in `dbt_project/models/{layer}/`.
2. Description + tests in the matching `schema.yml`.
3. New seeds keep **ASCII-only** content (Dremio Calcite chokes on Unicode
   literals in seed loads — lesson from PR #18). Rich descriptions go in
   `seeds/schema.yml`, not the CSV.
4. The `dbt-validate` CI job will catch most structural errors without
   needing a live Dremio.

## Adding a Python dependency

Add it to `pyproject.toml` and refresh the lockfile:

```bash
uv lock
```

Commit both files together with a `chore(deps)` message.

`pyspark`, `apache-airflow`, and `apache-airflow-providers-*` are pinned and
explicitly ignored by Dependabot — bumping them by hand goes with the
matching Dockerfile changes for classpath compatibility.

## Reporting a bug

Open an issue with:
- DAG / model / module name
- Full error message + stack trace (or scheduler log excerpt)
- What you ran (`make up` from scratch? trigger from UI? from the CLI?)
- Anything you've already ruled out

## Code of conduct

Be kind. Disagreements about technical choices are welcome; disagreements
about people aren't.
