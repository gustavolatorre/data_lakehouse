"""Airflow 3.2.1 DAG — Gold dbt Processing for Brasileirão.

Triggers when the iceberg_silver_brasileirao asset is updated.
Runs ``dbt build --select tag:brasileirao`` against Dremio — seeds (none),
models, and tests for the Brasileirão graph only, in topological order, with
the staging/mart tests acting as blocking quality gates.
Emits the iceberg_gold_brasileirao asset only if the build succeeds.

Scoped by the ``brasileirao`` tag (``--select tag:brasileirao``) so the build
targets only the Brasileirão graph, off its own Silver asset.
"""

import logging
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Asset, dag
from callbacks import build_failure_callback

logger = logging.getLogger("airflow.task")

local_tz = pendulum.timezone("America/Sao_Paulo")

# Reactive assets (inlet + outlet)
iceberg_silver_brasileirao = Asset("iceberg://nessie/silver/brasileirao")
iceberg_gold_brasileirao = Asset("iceberg://nessie/gold/brasileirao")

on_failure_callback = build_failure_callback("GOLD DBT BRASILEIRAO PROCESSING")

DBT_PROJECT_DIR = "/opt/airflow/dbt_project"


@dag(
    dag_id="gold_dbt_brasileirao_processing",
    description="Analytics layer: Iceberg Silver → dbt build (Gold classificação) on Dremio",
    schedule=iceberg_silver_brasileirao,  # asset-reactive schedule
    start_date=pendulum.datetime(2024, 1, 1, tz=local_tz),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=30),
        "on_failure_callback": on_failure_callback,
    },
    tags=["brasileirao", "gold", "dbt", "dremio"],
)
def gold_dbt_brasileirao_pipeline():
    """Execute a scoped ``dbt build`` to materialize the Brasileirão Gold mart."""

    BashOperator(
        task_id="dbt_build",
        # `--select tag:brasileirao` builds only the Brasileirão graph
        # (stg_silver_brasileirao → mart_classificacao_brasileirao) plus their
        # tests. `--target prod` pinned so a missing DBT_TARGET cannot route
        # prod into the gold_dev schema.
        bash_command=(
            f"dbt build --select tag:brasileirao --target prod "
            f"--profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}"
        ),
        outlets=[iceberg_gold_brasileirao],  # asset emitted only if the build passes
        execution_timeout=timedelta(minutes=20),
        retries=2,
        retry_delay=timedelta(minutes=3),
    )


# Instantiate the pipeline
gold_dbt_brasileirao_pipeline()
