"""MinIO client factory for S3-compatible object storage operations.

Provides a centralized client factory and helper functions for interacting
with MinIO buckets (bronze, silver, gold layers).
"""

import io
import json
import logging

from minio import Minio, S3Error

from src.config.settings import get_settings

logger = logging.getLogger(__name__)

# Staging bucket marker that the Bronze/Silver DAG's check_quarantine task polls.
STAGING_BUCKET = "staging"


def create_minio_client() -> Minio:
    """Create a MinIO client using application settings.

    Returns:
        Minio: Configured MinIO client instance.

    Raises:
        Exception: If the client cannot connect to MinIO.
    """
    settings = get_settings()

    client = Minio(
        settings.minio_endpoint,
        access_key=settings.minio_root_user,
        secret_key=settings.minio_root_password,
        secure=settings.minio_secure,
    )

    logger.info(
        "MinIO client created for endpoint '%s' (TLS=%s)",
        settings.minio_endpoint,
        settings.minio_secure,
    )
    return client


def ensure_bucket_exists(client: Minio, bucket_name: str) -> None:
    """Ensure a MinIO bucket exists, creating it if necessary.

    Args:
        client: MinIO client instance.
        bucket_name: Name of the bucket to verify/create.

    Raises:
        S3Error: If bucket creation fails due to permissions or connectivity.
    """
    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            logger.info("Bucket '%s' created", bucket_name)
        else:
            logger.debug("Bucket '%s' already exists", bucket_name)
    except S3Error:
        logger.exception("Failed to verify/create bucket '%s'", bucket_name)
        raise


def write_quarantine_alert(domain: str, execution_date: str, quarantined_rows: int, reason: str) -> None:
    """Drop a quarantine-alert marker in staging for the DAG to pick up.

    The Bronze/Silver DAG's ``check_quarantine`` task reads
    ``{domain}/{execution_date}/quarantine_alert.json`` after the Spark
    fan-out and raises an operational alert when ``quarantined_rows > 0``.
    Writing this marker is how the in-Spark quarantine event reaches the
    Airflow side (the two run in different containers).

    Best-effort by design: a failure to write the marker is logged but never
    raised, so a MinIO hiccup cannot fail an otherwise-successful Silver run.

    Args:
        domain: Pipeline domain / staging prefix (e.g. ``"brasileirao"``).
        execution_date: Run date (YYYY-MM-DD); keys the marker object.
        quarantined_rows: How many rows were diverted to quarantine.
        reason: Stable reason code (e.g. ``"NULL_ID"``).
    """
    object_name = f"{domain}/{execution_date}/quarantine_alert.json"
    try:
        client = create_minio_client()
        payload = {
            "quarantined_rows": quarantined_rows,
            "reason": reason,
            "execution_date": execution_date,
        }
        data = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        client.put_object(
            bucket_name=STAGING_BUCKET,
            object_name=object_name,
            data=io.BytesIO(data),
            length=len(data),
            content_type="application/json",
        )
        logger.info("Quarantine alert marker written to staging: %s", object_name)
    except Exception:
        logger.exception("Failed to write quarantine alert marker %s", object_name)


def clear_quarantine_alert(domain: str, execution_date: str) -> None:
    """Remove a stale quarantine-alert marker after a clean run.

    Without this, a re-run of an ``execution_date`` whose data is now 100%
    clean would leave the marker from the previous (dirty) run in staging,
    and the DAG's ``check_quarantine`` task would fire a false alert.

    Best-effort by design, mirroring :func:`write_quarantine_alert`: failures
    are logged but never raised. S3 ``DeleteObject`` semantics make removing a
    nonexistent key a no-op, so the happy path (no marker ever written) is
    silent.

    Args:
        domain: Pipeline domain / staging prefix (e.g. ``"brasileirao"``).
        execution_date: Run date (YYYY-MM-DD); keys the marker object.
    """
    object_name = f"{domain}/{execution_date}/quarantine_alert.json"
    try:
        client = create_minio_client()
        client.remove_object(STAGING_BUCKET, object_name)
        logger.info("Quarantine alert marker cleared (if present): %s", object_name)
    except Exception:
        logger.exception("Failed to clear quarantine alert marker %s", object_name)
