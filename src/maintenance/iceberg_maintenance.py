"""Iceberg table maintenance — compaction, snapshot expiration, orphan cleanup.

Run as a standalone Spark job (submitted by the ``iceberg_maintenance`` DAG).
For each Bronze and Silver table:
1. ``rewrite_data_files`` — bin-pack small files into right-sized ones.
2. ``expire_snapshots`` — drop snapshots older than the retention window, while
   keeping at least ``--min-snapshots`` recent ones.
3. ``remove_orphan_files`` — delete data files no longer referenced by any
   snapshot (with a safety interval, see Iceberg docs).
"""

import argparse
import logging
import sys
from datetime import UTC, datetime, timedelta

from pyspark.sql import SparkSession

from src.utils.spark_session import create_spark_session

logger = logging.getLogger(__name__)

MAINTAINED_TABLES = [
    # brasileirao pipeline — Bronze + Silver + quarantine. Without these the
    # Iceberg tables accumulate small files + unbounded snapshots, since
    # nothing else expires their history.
    "nessie.bronze.brasileirao",
    "nessie.silver.brasileirao",
    "nessie.silver.brasileirao_quarantine",
]


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--retention-days",
        type=int,
        default=30,
        help="Snapshot retention window in days (default: 30).",
    )
    parser.add_argument(
        "--min-snapshots",
        type=int,
        default=5,
        help="Minimum number of recent snapshots to always keep (default: 5).",
    )
    return parser.parse_args(argv)


def _enable_gc(spark: SparkSession, table: str) -> None:
    """Allow ``expire_snapshots`` + ``remove_orphan_files`` to delete files.

    Iceberg defaults ``gc.enabled`` to ``false`` as a safety guard against
    shared/external metadata. We own these tables exclusively. After P3.13
    new tables are created with ``gc.enabled=true`` directly in DDL, so this
    call is a no-op for those — but we keep it as a defensive net for any
    pre-P3.13 table that's already in the warehouse and never got the flag.
    """
    logger.info("Ensuring GC enabled on %s (gc.enabled=true)", table)
    spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('gc.enabled'='true')")


def _rewrite_data_files(spark: SparkSession, table: str) -> None:
    """Run Iceberg's bin-pack compaction on ``table``."""
    logger.info("Rewriting data files: %s", table)
    spark.sql(f"CALL nessie.system.rewrite_data_files(table => '{table}')")


def _expire_snapshots(
    spark: SparkSession,
    table: str,
    retention_days: int,
    min_snapshots: int,
) -> None:
    """Expire snapshots older than ``retention_days`` while keeping ``min_snapshots``."""
    threshold = datetime.now(UTC) - timedelta(days=retention_days)
    threshold_str = threshold.strftime("%Y-%m-%d %H:%M:%S.%f")
    logger.info(
        "Expiring snapshots: %s (older than %s, keep min %d)",
        table,
        threshold_str,
        min_snapshots,
    )
    spark.sql(
        f"CALL nessie.system.expire_snapshots("
        f"table => '{table}', "
        f"older_than => TIMESTAMP '{threshold_str}', "
        f"retain_last => {min_snapshots})"
    )


def _remove_orphan_files(spark: SparkSession, table: str) -> None:
    """Remove data files not referenced by any snapshot of ``table``."""
    logger.info("Removing orphan files: %s", table)
    # The default older_than is now() - 3 days, which is the safe window
    # against in-flight writes. We accept the default here.
    spark.sql(f"CALL nessie.system.remove_orphan_files(table => '{table}')")


def run_maintenance(retention_days: int, min_snapshots: int) -> None:
    """Run the full maintenance sweep on all configured Iceberg tables."""
    spark = create_spark_session("IcebergMaintenance")

    try:
        for table in MAINTAINED_TABLES:
            if not spark.catalog.tableExists(table):
                # Quarantine + snapshot tables are only created on demand
                # (when an invalid row is rejected, when dbt runs, etc.).
                # Skipping a missing table is preferable to making the whole
                # weekly DAG red.
                logger.info("Skipping %s — table does not exist yet", table)
                continue
            _enable_gc(spark, table)
            _rewrite_data_files(spark, table)
            _expire_snapshots(spark, table, retention_days, min_snapshots)
            _remove_orphan_files(spark, table)
            logger.info("Maintenance complete for %s", table)
    finally:
        spark.stop()
        logger.info("SparkSession stopped")


if __name__ == "__main__":
    from src.utils.logging_config import setup_logging

    setup_logging()
    args = _parse_args(sys.argv[1:])
    run_maintenance(args.retention_days, args.min_snapshots)
