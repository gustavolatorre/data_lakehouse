"""Unit tests for ``src.utils.minio_client``."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from minio.error import S3Error

from src.utils.minio_client import (
    clear_quarantine_alert,
    create_minio_client,
    ensure_bucket_exists,
    write_quarantine_alert,
)


@patch("src.utils.minio_client.get_settings")
@patch("src.utils.minio_client.Minio")
def test_create_minio_client_uses_settings(mock_minio_cls, mock_get_settings):
    """create_minio_client must wire Settings into the Minio constructor."""
    mock_get_settings.return_value = MagicMock(
        minio_endpoint="example:9000",
        minio_root_user="alice",
        minio_root_password="s3cret",  # pragma: allowlist secret
        minio_secure=True,
    )

    create_minio_client()

    mock_minio_cls.assert_called_once_with(
        "example:9000",
        access_key="alice",
        secret_key="s3cret",  # pragma: allowlist secret
        secure=True,
    )


@patch("src.utils.minio_client.get_settings")
@patch("src.utils.minio_client.Minio")
def test_create_minio_client_defaults_to_insecure(mock_minio_cls, mock_get_settings):
    """A False minio_secure setting must propagate as secure=False."""
    mock_get_settings.return_value = MagicMock(
        minio_endpoint="local:9000",
        minio_root_user="u",
        minio_root_password="p",
        minio_secure=False,
    )

    create_minio_client()

    _, kwargs = mock_minio_cls.call_args
    assert kwargs["secure"] is False


def test_ensure_bucket_exists_creates_when_missing():
    """ensure_bucket_exists must call make_bucket when the bucket is absent."""
    client = MagicMock()
    client.bucket_exists.return_value = False

    ensure_bucket_exists(client, "my-bucket")

    client.bucket_exists.assert_called_once_with("my-bucket")
    client.make_bucket.assert_called_once_with("my-bucket")


def test_ensure_bucket_exists_skips_when_present():
    """ensure_bucket_exists must NOT call make_bucket when the bucket exists."""
    client = MagicMock()
    client.bucket_exists.return_value = True

    ensure_bucket_exists(client, "already-here")

    client.bucket_exists.assert_called_once_with("already-here")
    client.make_bucket.assert_not_called()


def test_ensure_bucket_exists_reraises_s3error():
    """Any S3Error during bucket existence/creation must propagate to the caller."""
    client = MagicMock()
    client.bucket_exists.side_effect = S3Error(
        code="AccessDenied",
        message="nope",
        resource="my-bucket",
        request_id="rid",
        host_id="hid",
        response=MagicMock(status=403),
    )

    with pytest.raises(S3Error):
        ensure_bucket_exists(client, "my-bucket")


@patch("src.utils.minio_client.create_minio_client")
def test_write_quarantine_alert_puts_marker(mock_create):
    """The marker is written to staging/<domain>/<date>/quarantine_alert.json."""
    client = MagicMock()
    mock_create.return_value = client

    write_quarantine_alert("brasileirao", "2026-05-30", 3, "NULL_GE_MATCH_ID")

    _, kwargs = client.put_object.call_args
    assert kwargs["bucket_name"] == "staging"
    assert kwargs["object_name"] == "brasileirao/2026-05-30/quarantine_alert.json"
    # Payload carries the count + reason so the DAG task can decide to alert.
    import json

    kwargs["data"].seek(0)
    payload = json.loads(kwargs["data"].read().decode("utf-8"))
    assert payload == {"quarantined_rows": 3, "reason": "NULL_GE_MATCH_ID", "execution_date": "2026-05-30"}


@patch("src.utils.minio_client.create_minio_client")
def test_write_quarantine_alert_swallows_errors(mock_create):
    """A MinIO failure must never bubble up and fail the Silver job."""
    client = MagicMock()
    client.put_object.side_effect = OSError("minio down")
    mock_create.return_value = client

    # Must not raise.
    write_quarantine_alert("brasileirao", "2026-05-30", 1, "NULL_ID")


@patch("src.utils.minio_client.create_minio_client")
def test_clear_quarantine_alert_removes_marker(mock_create):
    """A clean run removes the stale marker so check_quarantine stays quiet."""
    client = MagicMock()
    mock_create.return_value = client

    clear_quarantine_alert("brasileirao", "2026-05-30")

    client.remove_object.assert_called_once_with("staging", "brasileirao/2026-05-30/quarantine_alert.json")


@patch("src.utils.minio_client.create_minio_client")
def test_clear_quarantine_alert_swallows_errors(mock_create):
    """Mirror of the write path: a MinIO failure must never fail the Silver job."""
    client = MagicMock()
    client.remove_object.side_effect = OSError("minio down")
    mock_create.return_value = client

    # Must not raise.
    clear_quarantine_alert("brasileirao", "2026-05-30")
