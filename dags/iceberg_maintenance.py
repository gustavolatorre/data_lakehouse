"""Airflow 3.2.1 DAG — Iceberg table maintenance.

Runs weekly to keep the Bronze and Silver Iceberg tables healthy:
- ``rewrite_data_files`` compacts small files into right-sized ones, reducing
  read amplification on the next pipeline runs.
- ``expire_snapshots`` reclaims storage by removing snapshots older than the
  retention window, while keeping the minimum required for time-travel.
- ``remove_orphan_files`` cleans up data files no longer referenced by any
  snapshot (typically left behind by failed writes).

These are non-destructive when configured correctly: time-travel within the
retention window still works, and the current snapshot of each table is always
preserved.
"""

import logging
from datetime import timedelta

import pendulum
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import dag
from callbacks import build_failure_callback

logger = logging.getLogger("airflow.task")

local_tz = pendulum.timezone("America/Sao_Paulo")

SPARK_CONF = {
    "spark.driver.memory": "2g",
    "spark.executor.memory": "2g",
    "spark.executor.instances": "1",
}

# Default snapshot retention settings. These values are used as fallback defaults
# in the Jinja templates, which pull dynamically from Airflow Variables:
#   * iceberg_snapshot_retention_days (default: 30)
#   * iceberg_min_snapshots_to_keep (default: 5)

# Shared single-slot Spark pool (B-02). EVERY Spark task in the project — the
# daily Bronze/Silver SparkSubmits for both pipelines AND this weekly
# maintenance job — runs in `spark_worker` (slots=1), so Airflow lets only one
# of them submit to the single 2g Spark worker at a time and queues the rest.
# That is what actually prevents the concurrent-OOM: a *separate* maintenance
# pool (the old design) only serialized maintenance against itself, not against
# the daily ETL, which still ran on the same worker.
# Provisioned at scheduler startup — see docker-compose.yml scheduler command:
# `airflow pools set spark_worker 1 ...`.
SPARK_WORKER_POOL = "spark_worker"

on_failure_callback = build_failure_callback("ICEBERG MAINTENANCE")


@dag(
    dag_id="iceberg_maintenance",
    description="Weekly maintenance: rewrite data files, expire snapshots, remove orphan files",
    schedule="@weekly",
    start_date=pendulum.datetime(2024, 1, 1, tz=local_tz),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "depends_on_past": False,
        # Three retries with exponential backoff (10m → 20m → 40m). Compaction
        # failures are most often transient (Nessie connection wobble or
        # transient Spark worker pressure), and we want the run to recover
        # without paging anyone on a weekend.
        "retries": 3,
        "retry_delay": timedelta(minutes=10),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=60),
        "execution_timeout": timedelta(hours=1),
        "on_failure_callback": on_failure_callback,
    },
    tags=["iceberg", "maintenance", "weekly"],
)
def iceberg_maintenance_pipeline():
    """Schedule maintenance procedures across Bronze and Silver tables."""
    SparkSubmitOperator(
        task_id="run_iceberg_maintenance",
        conn_id="spark_docker",
        application="/opt/airflow/src/maintenance/iceberg_maintenance.py",
        application_args=[
            "--retention-days",
            "{{ var.value.get('iceberg_snapshot_retention_days', '30') }}",
            "--min-snapshots",
            "{{ var.value.get('iceberg_min_snapshots_to_keep', '5') }}",
        ],
        conf=SPARK_CONF,
        execution_timeout=timedelta(minutes=45),
        # Shared single-slot Spark pool — Airflow queues this until no other
        # Spark job (daily ETL or maintenance) holds the worker. See
        # SPARK_WORKER_POOL above.
        pool=SPARK_WORKER_POOL,
    )


iceberg_maintenance_pipeline()
