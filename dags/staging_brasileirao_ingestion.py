"""Airflow 3.2.1 DAG — Brasileirão Staging Ingestion.

Fetches Brasileirão Série A matches data from web (UOL/GE/CBF) and uploads raw JSON
files to the MinIO Staging bucket. Emits the staging_brasileirao_raw asset.
"""

import logging
from datetime import timedelta

import pendulum
from airflow.sdk import Asset, dag, task
from callbacks import build_failure_callback

logger = logging.getLogger("airflow.task")

local_tz = pendulum.timezone("America/Sao_Paulo")

# Reactive output asset
staging_brasileirao_raw = Asset("s3://staging/brasileirao")

on_failure_callback = build_failure_callback("STAGING BRASILEIRAO")


@dag(
    dag_id="staging_brasileirao_ingestion",
    description="Ingestion layer: Web Scraper (Brasileirão) → MinIO Staging",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 4, 13, tz=local_tz),  # Approximate start of Brasileirao 2026
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
    tags=["brasileirao", "futebol", "staging", "ingestion"],
)
def staging_brasileirao_pipeline():
    """Ingest Brasileirão match data from web and emit staging_brasileirao_raw asset."""

    @task(outlets=[staging_brasileirao_raw])
    def fetch_to_staging(**context) -> int:
        """Fetch Brasileirão data from Web and upload to MinIO Staging bucket.

        Returns:
            Number of matches fetched and uploaded.
        """
        # We use `ds` for logical date processing (D-1 by default for daily DAGs at midnight)
        execution_date = context.get("ds") or pendulum.now(local_tz).strftime("%Y-%m-%d")
        logger.info("Starting Staging ingestion for Brasileirão, date=%s", execution_date)

        # Import locally inside the task to prevent DAG parsing timeouts
        from src.staging.fetch_brasileirao import fetch_and_upload

        total = fetch_and_upload(execution_date)
        logger.info("Brasileirão staging ingestion complete: %d records", total)
        return total

    fetch_to_staging()


# Instantiate the ingestion pipeline
staging_brasileirao_pipeline()
