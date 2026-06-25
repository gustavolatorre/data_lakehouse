# Architecture

> Living document. Update whenever the topology, layer contract, or major
> integration changes. The README is the marketing/onboarding view; this is
> the engineering view.

## 1. High-level topology

```
                            ┌────────────────────┐
                            │  GE / Globo Esporte │
                            │   internal JSON API │
                            └─────────┬──────────┘
                                      │ HTTPS, 38 rounds × season
                                      ▼
        ┌──────────┐   put_object   ┌─────────┐   s3a://staging/   ┌────────────┐
        │  Airflow │───────────────▶│  MinIO  │◀───────────────────│  Spark 4   │
        │   3.2.1  │                │ (staging│                    │ (master +  │
        │ Scheduler│                │ warehs.)│   s3a://warehouse/ │   worker)  │
        │ + DAG    │◀────────────── │         │ ─────────────────▶ │            │
        │ Processor│   asset event  └─────────┘    Iceberg files   └─────┬──────┘
        │ + API Srv│                                                      │
        └────┬─────┘                                                      │ register tables
             │ submit                                                     ▼
             ▼                                                      ┌─────────┐
        ┌─────────┐   read/write    ┌─────────┐    REST API v2      │  Nessie │
        │  Spark  │────────────────▶│ Iceberg │◀───────────────────▶│ catalog │
        │ submits │                 │ tables  │                     └────┬────┘
        └─────────┘                 └─────────┘                          │ source
                                         ▲                               ▼
                                         │ dbt build               ┌──────────┐
                                    ┌────┴────┐                     │  Dremio  │
                                    │  dbt-   │◀──── SQL (TCP) ──────│ 25.0.0   │
                                    │ dremio  │                     └──────────┘
                                    └─────────┘
```

11 services in `docker-compose.yml`: 9 long-running containers + 2 one-shot
setup containers (`minio-setup`, `dremio-setup`).

## 2. Layer contract (Medallion)

| Layer | Path | Engine | Schema discipline | Idempotency |
|-------|------|--------|-------------------|-------------|
| Staging | `s3://staging/brasileirao/{match_date}/matches.json` | Python + MinIO SDK | Raw JSON | Full reconcile — (re)stage every finished match `<= execution_date`, one file per match-date, overwritten idempotently |
| Bronze | `nessie.bronze.brasileirao` (Iceberg v2) | Spark | Explicit schema + `ingestion_ts` for partition spec | `overwritePartitions()` on `days(ingestion_ts)`; full multi-date scan, every staged date re-ingested (no watermark) |
| Silver | `nessie.silver.brasileirao` (Iceberg v2) | Spark `MERGE INTO` | Trimmed analytical schema | Full-Bronze upsert on `ge_match_id` (no watermark); partitioned by `months(match_date)` |
| Gold | `lakehouse.gold.mart_classificacao_brasileirao` (Dremio) | dbt-dremio | `schema.yml` per model | `CREATE OR REPLACE` table; one row per `(season, time_)`, ranked per season |

> **Full reconcile (why no watermarks).** Every layer re-processes the complete
> current state each run rather than an `execution_date` slice. An earlier
> date-watermark design (`ingestion_date >= MAX`, `ingested_at >= MAX`) silently
> dropped **postponed / late-finalised** matches — a game played weeks after its
> scheduled round, or a score finalised out of order, has a *played date* before
> the latest already-processed date, so the watermark skipped it forever (this is
> how round 18 went missing while teams showed 17 games). Reconciling the whole
> set is cheap here (~one small JSON per match-date; a few hundred rows total)
> and every write is idempotent (`overwritePartitions` per day, `MERGE` upsert),
> so out-of-order arrivals can never be lost.

### Staging guarantees
- **Multi-season.** Editions come from `config/brasileirao_seasons.yml`
  (`season → campeonato_id / fase_slug`); `load_seasons()` ingests every entry,
  optionally filtered by `GE_SEASONS` (CSV of years). See §10.
- **Fail-loud.** If any round fails after retries, `_fetch_all_matches` raises
  `RuntimeError` rather than stage a partial season that would pass the Bronze
  `row_count >= 1` gate.
- Only **finished** matches are staged (official score present **and** kicked off).

### Bronze guarantees
- **Path-driven, not execution-date-driven.** A single staging run can drop many
  dates at once, so Bronze scans `s3a://staging/brasileirao/*/matches.json`,
  derives `ingestion_date` from the file path, and re-ingests every date via
  `overwritePartitions()`.
- Hidden partitioning by `days(ingestion_ts)` (Iceberg transform); `ingestion_ts`
  derives from the path date so re-runs land on the same partition.
- `ge_match_id` is a UUID-style **string** (not an int).
- Cached before count-driven validations to avoid N rescans.

### Silver guarantees
- **Plain UPSERT** keyed on `ge_match_id` — **no** `WHEN NOT MATCHED BY SOURCE`
  (a played match never disappears) and **no** shrink guard. MERGEs the entire
  Bronze table every run; re-processing already-seen matches is a no-op.
- **Enrichment**: derives `stadium_state` (UF) + `stadium_state_origin` via
  broadcast-joined lookup dicts (`src/silver/stadium_enrichment.py`), cascade
  `stadium → home_team → __UNKNOWN__`. Also derives `total_goals` and
  `match_outcome` (HOME_WIN / AWAY_WIN / DRAW).
- **Quarantine sink** (`nessie.silver.brasileirao_quarantine`, append-only,
  partitioned by `quarantine_date`): rows with NULL `ge_match_id` are diverted
  here instead of aborting the run, tagged with a stable `quarantine_reason`
  (`NULL_GE_MATCH_ID`).

### Gold guarantees
- `dbt build` (not `dbt run` then `dbt test`) — tests are blocking and run
  topologically per resource. A failing test on `stg_silver_brasileirao` skips
  the downstream `int_*` / `mart_*` models.
- Grain uniqueness on `(season, time_)` is enforced by the singular test
  `assert_classificacao_unique_season_team`.
- The Silver source intentionally has **no freshness SLA** (the football
  calendar has multi-week planned gaps — FIFA dates, the World Cup window, the
  off-season).

## 3. Orchestration

A single Medallion pipeline (full Bronze→Gold) plus a weekly maintenance DAG.
All inter-layer scheduling is asset-aware (reactive):

| DAG | Schedule | Outlets | Inlets |
|-----|----------|---------|--------|
| `staging_brasileirao_ingestion` | `@daily` | `s3://staging/brasileirao` | — |
| `bronze_silver_brasileirao_processing` | asset `s3://staging/brasileirao` | `iceberg://nessie/silver/brasileirao` (emitted by `merge_branch`) | (asset) |
| `gold_dbt_brasileirao_processing` | asset `iceberg://nessie/silver/brasileirao` | `iceberg://nessie/gold/brasileirao` | (asset) |
| `iceberg_maintenance` | `@weekly` | — | — |

Failure callbacks: shared factory in `dags/callbacks.py`
(`build_failure_callback("LAYER NAME")`).

Spark connection is wired via env var (`AIRFLOW_CONN_SPARK_DOCKER` on
`x-airflow-common`), not a plugin — Airflow 3 forbids ORM writes during
plugin `on_load`. Every Spark task runs in the single-slot `spark_worker` pool
so only one job hits the 2GB worker at a time.

## 4. Catalog / version control

Project Nessie is the transactional catalog. It gives Bronze and Silver
**isolated, Git-like branching**: every Bronze/Silver DAG run carves out its own
Nessie branch before any Spark write and merges it back into `main` only after
the Silver MERGE succeeds.

```
main ───────────────────────────────────────●─────▶ (Gold reads here)
                                            ▲
                                            │ merge_branch (success path)
            create_branch                   │
main ──●── etl_bronze_silver_brasileirao_<date> ┘
       └──────▶  staging_to_bronze ▶ bronze_to_silver ▶ merge_branch
                                          │
                                          ▼ on any failure
                                    cleanup_branch
                                    (drops the branch — main untouched)
```

`src/utils/nessie_branch.py` hits the Nessie v2 REST API directly (no
`nessie-spark-extensions` JAR needed). Branch names are deterministic —
`etl_bronze_silver_brasileirao_YYYY_MM_DD` — so re-runs are idempotent: the
second attempt re-uses the existing branch instead of failing.

`create_spark_session(app_name, nessie_ref=...)` binds the catalog to a
specific ref; `ingest_brasileirao.py` and `transform_brasileirao.py` accept a
`--nessie-ref` CLI flag that the DAG fills in from a Jinja template. Gold
(dbt-dremio) continues to read `main`.

Iceberg `format-version=2` everywhere. v3 was attempted but Dremio 25.0.0
cannot read it; will revisit when Dremio adds support.

## 5. Quality gates

| Gate | Where | What it catches |
|------|-------|-----------------|
| Pre-commit | local | Format, lint, large files, secrets |
| `Lint` CI job | PR | Ruff (`check` + `format --check`), mypy |
| `Test` CI job | PR | Unit tests, coverage gate `fail_under=85` |
| `Integration` CI job | PR | e2e Bronze→Silver suite + DagBag import (Java 17 + Iceberg runtime JAR) |
| `dbt Validate` CI job | PR | `dbt deps + parse` via `dbt-duckdb` (connectionless) |
| `Security` CI job | PR | Trivy fs **HIGH+CRITICAL fixable = blocking**; `pip-audit` + Trivy config informational |
| Fail-loud partial fetch | Staging Python | A failed round raises rather than staging a partial season |
| Quarantine split (`ge_match_id IS NULL`) | Silver Spark job | Diverts bad rows to `brasileirao_quarantine` instead of aborting |
| Declarative YAML rules via `run_quality_checks` | Bronze (pre-write) + Silver (post-MERGE) Spark jobs | Row count, missing-count, unique-count, missing-percent — `fail` rules abort, `warn` rules only log |
| `dbt build` tests (`unique`, `not_null`, singular grain test) | Gold Airflow task | Cross-layer integrity |

## 5b. Observability — OpenLineage

Two emitters, with **different empty-URL behaviour**. Both keep the bundled
images working with no external dependency:

| Emitter | Source | When `OPENLINEAGE_URL` is empty (default) |
|---------|--------|-------------------------------------------|
| Airflow provider | `apache-airflow-providers-openlineage` (image-baked) | Provider stays loaded, log-only — nothing transmitted |
| Spark listener | `openlineage-spark_2.13` JAR + `spark.extraListeners` | **Not registered at all** — `_apply_openlineage_config` returns the builder untouched (silent no-op) |

**Why the Spark listener is not registered when empty:** the Spark driver runs
`spark-submit` in client mode from the Airflow container, where the listener
JAR is *not* on the classpath. Registering `spark.extraListeners`
unconditionally would crash the driver with `ClassNotFoundException`. So
`OPENLINEAGE_URL` doubles as the opt-in switch — set it and you also take
responsibility for making the JAR reachable on the driver (e.g. via `--jars`).
Point at Marquez / any OpenLineage-compatible collector when ready.
`alert_webhook_url` follows the same opt-in pattern for DAG-failure / quarantine
alerts.

## 6. Credentials & secrets

Local development only. Three categories:

| Where | What |
|-------|------|
| `.env` (gitignored) | MinIO, Postgres, Dremio admin creds; `GE_SEASONS` filter |
| `airflow.env` (gitignored) | Fernet key, API secret key, JWT secret, DB SQLAlchemy URL |
| `simple_auth_passwords.json` (container-internal, never mounted) | Plaintext Airflow login password — SimpleAuthManager does NOT support hashing |

Bootstrap helper: `make init-secrets` copies `airflow.env.example` to
`airflow.env` with freshly generated Fernet + Webserver + JWT keys.

## 7. Where to look when something breaks

| Symptom | First place to check |
|---------|----------------------|
| Login 401 on `localhost:8080` | `simple_auth_passwords.json` inside the api-server container; `AIRFLOW_USER` / `AIRFLOW_PASSWORD` in `.env` |
| Task "zombie" (state queued + executor failed) | JWT secret mismatch between containers — `AIRFLOW__API_AUTH__JWT_SECRET` in `airflow.env` |
| Staging task raises `GE API fetch incomplete` | A round failed after retries — inspect the GE endpoint / `config/brasileirao_seasons.yml` UUIDs before clearing |
| `dremio-setup` crash-loop with `syntax error: unexpected word` | CRLF line endings in `setup_sources.sh` (Windows checkout without `.gitattributes` LF coercion) |
| A team shows one game fewer than expected | A postponed match's played date precedes the latest processed date — full reconcile handles it; check the staging file for that match-date exists |
| Rows missing from Silver but present in Bronze | Check `nessie.silver.brasileirao_quarantine WHERE quarantine_date = '<date>'` — they probably had NULL `ge_match_id` |
| A future 2027 edition blends into 2026 standings | It won't — `season = year(match_date)` keeps editions separate; confirm match dates are correct |

## 8. Gold — league table (classificação)

Modeled in the dbt project, tagged `brasileirao`:

- `stg_silver_brasileirao` (view) — reads `lakehouse.silver.brasileirao AT
  BRANCH main` and derives `season = extract(year from match_date)`.
- `int_brasileirao_partidas` (view, intermediate) — the team-match grain: every
  match expanded into a home-perspective and an away-perspective row (with
  `mando`, goals for/against, win/draw/loss flags, points). The reusable base
  for any per-team mart.
- `mart_classificacao_brasileirao` (table) — aggregates the intermediate into
  one row per `(season, time_)` (jogos, V/E/D, GP/GC, saldo, pontos,
  aproveitamento) and `RANK() OVER (PARTITION BY season …)` on the official
  tie-break (points → wins → goal difference → goals for). Materialized as a
  full `table` — at ~20 rows/season the rebuild is trivial.

The Gold DAG runs `dbt build --select tag:brasileirao`; new models must carry
`{{ config(tags=['brasileirao']) }}` to be selected.

## 9. Design decision — deferred Gold branch isolation

Bronze/Silver already run on isolated Nessie branches (§4); Gold/dbt still
builds directly against `main`. Extending the `create_branch → build → merge`
pattern to Gold would give the analytics layer the same atomic-rollback
guarantee. Investigated and **deliberately not shipped** — a live spike settled
the feasibility in two halves:

- **Dremio supports it.** `CREATE BRANCH … IN lakehouse`, `CREATE TABLE … AT
  BRANCH <b> AS …`, branch isolation, and `MERGE BRANCH <b> INTO main` all work;
  reads already use `AT BRANCH main`.
- **dbt-dremio 1.10 does not expose it.** `DremioCredentials` has no
  branch/reference field; `DremioRelation.render()` emits no `AT BRANCH`; and
  every write macro renders a bare `{{ relation }}`. Branch-qualifying the
  *writes* would mean monkey-patching the adapter's relation rendering
  (version-locked) and making its metadata layer branch-aware.

**Decision:** not shipped. Bridging that gap is brittle, version-locked adapter
surgery — disproportionate for the small blast radius (a mostly `CREATE OR
REPLACE` Gold table, self-healed on the next build, with per-resource `dbt
build` test gates already skipping downstream models on an upstream test
failure). Revisit when dbt-dremio ships native Nessie-branch support.

## 10. Season turnover (current-season auto-derivation)

The Brasileirão championship UUID is **stable** across editions (`d1a37fa4…`,
verified live 2026-06-24); only the phase slug changes per year, deterministically
(`fase-unica-campeonato-brasileiro-{year}`). So
`src/staging/fetch_brasileirao.py::load_seasons(execution_date)` **derives the
active edition from the run's year** — the pipeline rolls over to the next
season automatically, with no config edit. `GE_SEASONS` (CSV of years) is an
optional override to pin/force editions. For the selected season it fetches all
38 rounds via `build_round_url(rodada, season)` and groups finished matches by
match-date; because staging partitions by match date, Silver upserts on the
globally-unique `ge_match_id`, and Gold ranks per `season`, **nothing downstream
changes** at turnover.

**GE serves only the active season.** Verified live: the public GE `/tabela/`
endpoint returns HTTP 500 for past editions (2020–2025). Historical backfill is
therefore **not possible** through this source; it would need a different source
(CBF scraping, datasets) and is out of scope. (The official Globo SDE API does
address past editions but is an internal host — `api.sde.globoi.com` — that
doesn't resolve publicly and is token-gated.)

**Off-season handling.** If the new edition isn't published yet (round 1 errors
after retries), the fetch treats it as "no active edition" (0 matches + `warn`)
rather than reddening the daily DAG. A genuine mid-season partial fetch (round 1
OK, a later round fails) still raises `RuntimeError`. New assumption **A-006**:
the slug pattern + stable UUID stay constant — mitigated by the
off-season-vs-failure distinction and the `GE_SEASONS` override.

`season = year(match_date)` in Gold is correct for modern editions (single
calendar year). The only exception is the 2020 edition (finished Feb 2021), but
since historical backfill is out of scope that's moot.
