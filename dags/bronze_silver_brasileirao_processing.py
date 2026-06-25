"""Airflow 3.2.1 DAG — Bronze & Silver Processing for Brasileirao with Nessie branching.

Triggers when the staging_brasileirao_raw asset is updated. Lifecycle:

    create_branch  ──▶  staging_to_bronze  ──▶  bronze_to_silver  ──▶  merge_branch
                                       │                                       │
                                       └──────────── cleanup_branch ◀──────────┘
                                              (runs only on upstream failure)

* The first task creates ``etl_bronze_silver_brasileirao_<date>`` off ``main``.
* Bronze and Silver Spark jobs bind to that branch via ``--nessie-ref``;
  every write stays isolated on that branch until ``merge_branch`` runs.
* ``merge_branch`` emits the asset only after the Silver MERGE + DQ contract
  has passed.
* ``cleanup_branch`` runs on upstream failure (``trigger_rule="one_failed"``)
  and drops the orphan branch so ``main`` stays clean.

Emits the iceberg_silver_brasileirao asset.
"""

import logging
from datetime import timedelta

import pendulum
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import Asset, dag, task
from airflow.task.trigger_rule import TriggerRule
from callbacks import build_failure_callback

logger = logging.getLogger("airflow.task")

local_tz = pendulum.timezone("America/Sao_Paulo")

# Reactive assets (inlet + outlet)
staging_brasileirao_raw = Asset("s3://staging/brasileirao")
iceberg_silver_brasileirao = Asset("iceberg://nessie/silver/brasileirao")

SPARK_CONF = {
    "spark.driver.memory": "2g",
    "spark.executor.memory": "2g",
    "spark.executor.instances": "1",
}

# Shared single-slot pool that serializes every Spark job across all DAGs onto
# the one 2GB worker (B-02). Must match the pool provisioned in
# docker-compose.yml (`airflow pools set spark_worker 1 ...`).
SPARK_WORKER_POOL = "spark_worker"

EXECUTION_DATE_TEMPLATE = (
    "{{ dag_run.logical_date.strftime('%Y-%m-%d') "
    "if (dag_run is defined and dag_run.logical_date is not none) "
    "else macros.datetime.now().strftime('%Y-%m-%d') }}"
)

NESSIE_BRANCH_TEMPLATE = (
    "etl_bronze_silver_brasileirao_{{ (dag_run.logical_date.strftime('%Y-%m-%d') "
    "if (dag_run is defined and dag_run.logical_date is not none) "
    "else macros.datetime.now().strftime('%Y-%m-%d'))"
    ".replace('-', '_') }}"
)

on_failure_callback = build_failure_callback("BRONZE/SILVER BRASILEIRAO PROCESSING")


@dag(
    dag_id="bronze_silver_brasileirao_processing",
    description="Processing layer with Nessie branch isolation: Staging → Bronze → Silver → MERGE",
    schedule=staging_brasileirao_raw,
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
    tags=["brasileirao", "bronze", "silver", "iceberg", "nessie-branching"],
)
def bronze_silver_brasileirao_pipeline():
    """Branch-isolated Bronze/Silver pipeline for Brasileirao."""

    @task(task_id="create_branch", retries=1)
    def create_branch(**context) -> str:
        """Create the isolated Nessie branch and return its name."""
        from src.utils.nessie_branch import build_branch_name
        from src.utils.nessie_branch import create_branch as _create

        execution_date = context.get("ds") or pendulum.now(local_tz).strftime("%Y-%m-%d")
        name = build_branch_name(dag_id="bronze_silver_brasileirao", execution_date=execution_date)
        logger.info("Creating isolated Nessie branch '%s'", name)
        _create(name, source_ref="main")
        return name

    branch_name = create_branch()

    staging_to_bronze = SparkSubmitOperator(
        task_id="staging_to_bronze",
        application="/opt/airflow/src/bronze/ingest_brasileirao.py",
        name="brasileirao_bronze_ingestion",
        conn_id="spark_docker",
        conf=SPARK_CONF,
        pool=SPARK_WORKER_POOL,
        application_args=[
            EXECUTION_DATE_TEMPLATE,
            "--nessie-ref",
            NESSIE_BRANCH_TEMPLATE,
        ],
        verbose=False,
    )

    bronze_to_silver = SparkSubmitOperator(
        task_id="bronze_to_silver",
        application="/opt/airflow/src/silver/transform_brasileirao.py",
        name="brasileirao_silver_transform",
        conn_id="spark_docker",
        conf=SPARK_CONF,
        pool=SPARK_WORKER_POOL,
        application_args=[
            EXECUTION_DATE_TEMPLATE,
            "--nessie-ref",
            NESSIE_BRANCH_TEMPLATE,
        ],
        verbose=False,
    )

    @task(task_id="merge_branch", outlets=[iceberg_silver_brasileirao], retries=1)
    def merge_branch(branch_ref: str) -> None:
        """Merge the isolated branch back into main upon success."""
        from src.utils.nessie_branch import merge_branch as _merge

        logger.info("Merging branch '%s' into 'main'", branch_ref)
        _merge(branch_ref, target="main")

    merge_task = merge_branch(branch_name)

    @task(task_id="cleanup_branch", trigger_rule=TriggerRule.ONE_FAILED, retries=1)
    def cleanup_branch(branch_ref: str) -> None:
        """Drop the branch if any upstream Spark job fails."""
        from src.utils.nessie_branch import drop_branch

        logger.warning("Upstream failure detected. Cleaning up branch '%s'", branch_ref)
        drop_branch(branch_ref)

    cleanup_task = cleanup_branch(branch_name)

    @task(task_id="check_quarantine", retries=2)
    def check_quarantine(**context) -> None:
        """Alert (not just log) when rows were quarantined for today's run.

        Reads the marker the Silver Spark job drops in staging and pages the
        alert sink when quarantined_rows > 0. Post-merge observability only —
        intentionally NOT an upstream of cleanup_branch (F-18): a failure here
        must never drop a branch that merge_branch already merged into main.
        """
        import json

        from minio.error import S3Error

        from src.utils.minio_client import create_minio_client

        execution_date = context.get("ds") or pendulum.now(local_tz).strftime("%Y-%m-%d")
        client = create_minio_client()
        object_name = f"brasileirao/{execution_date}/quarantine_alert.json"

        try:
            response = client.get_object("staging", object_name)
            try:
                data = json.loads(response.read().decode("utf-8"))
                rows = data.get("quarantined_rows", 0)
                reason = data.get("reason", "UNKNOWN")
                if rows > 0:
                    logger.error(
                        "SILVER QUARANTINE ALERT | execution_date=%s | quarantined_rows=%d | reason=%s | "
                        "Invalid rows diverted to nessie.silver.brasileirao_quarantine (NULL ge_match_id).",
                        execution_date,
                        rows,
                        reason,
                    )
                    from src.utils.alerting import send_alert

                    send_alert(
                        "Silver quarantine (brasileirao)",
                        f"execution_date={execution_date} quarantined_rows={rows} reason={reason} "
                        "table=nessie.silver.brasileirao_quarantine",
                        severity="error",
                    )
            finally:
                response.close()
                response.release_conn()
        except S3Error as e:
            if e.code == "NoSuchKey":
                logger.info("No quarantine alerts found for %s (happy path: clean data)", execution_date)
            else:
                logger.exception("Failed to check quarantine alert status in MinIO")

    check_quar = check_quarantine()

    # Topological execution order
    branch_name >> staging_to_bronze >> bronze_to_silver >> merge_task
    # check_quarantine is observability only — hangs off bronze_to_silver (runs
    # in parallel with merge_branch) and is NOT wired into cleanup (F-18): its
    # failure must never drop a branch that merge_branch already merged into main.
    bronze_to_silver >> check_quar
    # Cleanup runs if any of the Spark jobs OR the merge itself fail — a failed
    # merge_branch must not leave the etl_* branch orphaned on the Nessie server.
    [staging_to_bronze, bronze_to_silver, merge_task] >> cleanup_task


bronze_silver_brasileirao_pipeline()
