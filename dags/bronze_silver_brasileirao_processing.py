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

# Jinja templates that read create_branch's XCom return value (a dict). Every
# downstream consumer — both Spark jobs, merge, cleanup, check_quarantine — uses
# the SAME execution_date + branch name that create_branch computed *once*. This
# replaces the old per-task recompute from `dag_run.logical_date` (with a now()
# fallback in UTC), which could derive a DIFFERENT date/branch than create_branch
# (which uses America/Sao_Paulo) — across the midnight boundary or just any
# evening BRT run — leaving the Spark jobs writing to a branch create_branch
# never made and merge/cleanup acting on an orphan (A1).
XCOM_EXECUTION_DATE = "{{ ti.xcom_pull(task_ids='create_branch')['execution_date'] }}"
XCOM_NESSIE_BRANCH = "{{ ti.xcom_pull(task_ids='create_branch')['branch'] }}"

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
    def create_branch(**context) -> dict[str, str]:
        """Create the isolated Nessie branch and return its name + execution date.

        This is the **single source of truth** for both values: the execution
        date is resolved once (``ds``, falling back to ``now`` only when an
        asset-triggered run has no logical date) and the branch name is derived
        from it. Every downstream task consumes this XCom instead of recomputing,
        so the branch the Spark jobs write to can never diverge from the one
        created here / merged / cleaned up (A1).
        """
        from src.utils.nessie_branch import build_branch_name
        from src.utils.nessie_branch import create_branch as _create

        execution_date = context.get("ds") or pendulum.now(local_tz).strftime("%Y-%m-%d")
        name = build_branch_name(dag_id="bronze_silver_brasileirao", execution_date=execution_date)
        logger.info("Creating isolated Nessie branch '%s' (execution_date=%s)", name, execution_date)
        _create(name, source_ref="main")
        return {"execution_date": execution_date, "branch": name}

    cb = create_branch()

    staging_to_bronze = SparkSubmitOperator(
        task_id="staging_to_bronze",
        application="/opt/airflow/src/bronze/ingest_brasileirao.py",
        name="brasileirao_bronze_ingestion",
        conn_id="spark_docker",
        conf=SPARK_CONF,
        pool=SPARK_WORKER_POOL,
        application_args=[
            XCOM_EXECUTION_DATE,
            "--nessie-ref",
            XCOM_NESSIE_BRANCH,
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
            XCOM_EXECUTION_DATE,
            "--nessie-ref",
            XCOM_NESSIE_BRANCH,
        ],
        verbose=False,
    )

    @task(task_id="merge_branch", outlets=[iceberg_silver_brasileirao], retries=1)
    def merge_branch(branch_ref: str) -> None:
        """Merge the isolated branch back into main upon success."""
        from src.utils.nessie_branch import merge_branch as _merge

        logger.info("Merging branch '%s' into 'main'", branch_ref)
        _merge(branch_ref, target="main")

    merge_task = merge_branch(cb["branch"])

    @task(task_id="cleanup_branch", trigger_rule=TriggerRule.ONE_FAILED, retries=1)
    def cleanup_branch(branch_ref: str) -> None:
        """Drop the branch if any upstream Spark job fails."""
        from src.utils.nessie_branch import drop_branch

        logger.warning("Upstream failure detected. Cleaning up branch '%s'", branch_ref)
        drop_branch(branch_ref)

    cleanup_task = cleanup_branch(cb["branch"])

    @task(task_id="check_quarantine", retries=2)
    def check_quarantine(execution_date: str) -> None:
        """Alert (not just log) when rows were quarantined for today's run.

        Reads the marker the Silver Spark job drops in staging and pages the
        alert sink when quarantined_rows > 0. Post-merge observability only —
        intentionally NOT an upstream of cleanup_branch (F-18): a failure here
        must never drop a branch that merge_branch already merged into main.

        ``execution_date`` comes from create_branch's XCom so the marker path
        matches the date the Silver Spark job used — same single-source fix as
        the branch name (A1).
        """
        import json

        from minio.error import S3Error

        from src.utils.minio_client import create_minio_client

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

    check_quar = check_quarantine(cb["execution_date"])

    # Topological execution order
    cb >> staging_to_bronze >> bronze_to_silver >> merge_task
    # check_quarantine is observability only — hangs off bronze_to_silver (runs
    # in parallel with merge_branch) and is NOT wired into cleanup (F-18): its
    # failure must never drop a branch that merge_branch already merged into main.
    bronze_to_silver >> check_quar
    # Cleanup runs if any of the Spark jobs OR the merge itself fail — a failed
    # merge_branch must not leave the etl_* branch orphaned on the Nessie server.
    [staging_to_bronze, bronze_to_silver, merge_task] >> cleanup_task


bronze_silver_brasileirao_pipeline()
