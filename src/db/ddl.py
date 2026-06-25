"""Centralized Iceberg DDL for the Silver layer (F-02).

The Bronze→Silver transforms used to embed their ``CREATE TABLE`` statements
inline. Centralizing them here gives the table schemas a single home and keeps
the transform modules focused on transformation logic.

Each ``ensure_*`` function is idempotent — it issues
``CREATE NAMESPACE IF NOT EXISTS`` and then ``CREATE TABLE IF NOT EXISTS``
behind a ``catalog.tableExists`` guard — and preserves the Iceberg
``format-version=2`` + ``gc.enabled=true`` properties the maintenance jobs rely
on. The DDL strings are copied verbatim from the transforms, so relocating them
is behaviour-preserving (the existing mocked transform tests still assert the
same ``spark.sql`` calls).
"""

import logging

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

_SILVER_NAMESPACE = "nessie.silver"

# Fully-qualified table identifiers — the single source of truth for callers.
SILVER_BRASILEIRAO = "nessie.silver.brasileirao"
BRASILEIRAO_QUARANTINE = "nessie.silver.brasileirao_quarantine"


def _ensure_silver_namespace(spark: SparkSession) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {_SILVER_NAMESPACE}")


def ensure_silver_brasileirao(spark: SparkSession) -> None:
    """Create ``nessie.silver.brasileirao`` (the MERGE target) if absent."""
    _ensure_silver_namespace(spark)
    if spark.catalog.tableExists(SILVER_BRASILEIRAO):
        return
    logger.info("Creating Silver table %s for the first time", SILVER_BRASILEIRAO)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_BRASILEIRAO} (
            ge_match_id STRING,
            matchweek INT,
            match_date DATE,
            kickoff_ts TIMESTAMP,
            home_team STRING,
            home_team_code STRING,
            away_team STRING,
            away_team_code STRING,
            score_home INT,
            score_away INT,
            total_goals INT,
            match_outcome STRING,
            stadium STRING,
            stadium_state STRING,
            stadium_state_origin STRING,
            is_active BOOLEAN,
            updated_at TIMESTAMP,
            ingestion_date STRING
        )
        USING iceberg
        PARTITIONED BY (months(match_date))
        TBLPROPERTIES ('format-version'='2', 'gc.enabled'='true')
    """)


def ensure_brasileirao_quarantine(spark: SparkSession) -> None:
    """Create the brasileirao quarantine sink (append-only) if absent."""
    _ensure_silver_namespace(spark)
    if spark.catalog.tableExists(BRASILEIRAO_QUARANTINE):
        return
    logger.info("Creating quarantine table %s for the first time", BRASILEIRAO_QUARANTINE)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {BRASILEIRAO_QUARANTINE} (
            ge_match_id STRING,
            matchweek INT,
            home_team STRING,
            away_team STRING,
            date STRING,
            stadium STRING,
            ingestion_date STRING,
            ingested_at TIMESTAMP,
            quarantine_reason STRING,
            quarantined_at TIMESTAMP,
            quarantine_date STRING
        )
        USING iceberg
        PARTITIONED BY (quarantine_date)
        TBLPROPERTIES ('format-version'='2', 'gc.enabled'='true')
    """)
