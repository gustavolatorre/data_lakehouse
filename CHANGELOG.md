# Changelog

All notable changes to this project are documented here.
Format based on [Keep a Changelog](https://keepachangelog.com/),
versioning follows [Semantic Versioning](https://semver.org/).

## [3.2.0] - 2026-06-25

First public release — a focused, single-domain **Brasileirão Série A** medallion
data lakehouse on Airflow 3 + Spark 4 + Apache Iceberg/Nessie + dbt-on-Dremio + MinIO.

### Added
- **Multi-season ingestion** via current-season auto-derivation: the GE championship
  UUID is stable and the per-year `fase` slug is deterministic, so the active edition
  is derived from the run's year — no per-year config edit. `GE_SEASONS` overrides it;
  the off-season gap (edition not yet published) is treated as a benign no-op.
- **CodeQL** (Python SAST) workflow.
- **Trivy `image`** vulnerability scanning of the built Spark/Airflow images in the
  Docker Build workflow.
- `no-new-privileges` hardening on every docker-compose service.
- MIT `LICENSE`.

### Changed
- **Single-domain refactor**: removed the OpenBreweryDB ("breweries") pipeline, leaving
  the project focused on Brasileirão Série A (full Bronze → Silver → Gold). Rewrote
  README / ARCHITECTURE / CONTRIBUTING for the single domain.
- CI hardening: least-privilege `permissions: contents: read` and `concurrency`
  cancellation on the workflows.

### Fixed
- **Nessie branch isolation (A1):** the branch name + execution date are now propagated
  from `create_branch` via XCom (single source of truth). The previous per-task recompute
  from `dag_run.logical_date` (UTC `now()` fallback) could diverge from `create_branch`'s
  São Paulo derivation across the midnight boundary, orphaning the isolated branch.
- **Security (Trivy fs):** bumped `cryptography` and `python-multipart` to patched
  versions; risk-accepted the two unpatchable transitive `starlette` CVEs in `.trivyignore`
  with justification and a removal condition.

### Security
- Branch-protection ruleset on `main`: required PR + 1 approval, required status checks,
  no force-push / deletion.
- SHA-pinned GitHub Actions, `detect-secrets` pre-commit + baseline, secrets validation
  before `make up`.

[3.2.0]: https://github.com/gustavolatorre/data_lakehouse/releases/tag/v3.2.0
