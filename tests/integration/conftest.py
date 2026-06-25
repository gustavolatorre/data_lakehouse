# ruff: noqa: E402
"""Fixtures for integration tests.

Integration tests exercise the **real** Bronze and Silver code paths against
an Iceberg-on-local-filesystem warehouse, with no Nessie or MinIO involved.
A Hadoop catalog also called ``nessie`` lets the production SQL strings
(``nessie.bronze.brasileirao`` etc.) run unmodified — the production code
doesn't care that the catalog is backed by ``/tmp/...`` instead of a
Nessie service. Spark is configured via ``--packages`` so CI fetches the
Iceberg runtime JAR on the first run.

These fixtures are kept out of ``tests/conftest.py`` on purpose: the
existing unit-test fixture is a plain SparkSession without Iceberg
extensions, and we don't want every unit test to pay the cost of loading
the runtime + downloading the JAR.
"""

from __future__ import annotations

import os
import shutil
import sys
from unittest.mock import MagicMock

# Mock fcntl and Unix signals for Windows environment to allow Airflow import
if sys.platform == "win32":
    sys.modules["fcntl"] = MagicMock()
    import signal

    if not hasattr(signal, "SIGALRM"):
        signal.SIGALRM = 14
    if not hasattr(signal, "setitimer"):
        signal.setitimer = lambda *args, **kwargs: None
    if not hasattr(signal, "ITIMER_REAL"):
        signal.ITIMER_REAL = 0
    if not hasattr(signal, "alarm"):
        signal.alarm = lambda *args, **kwargs: None
    orig_signal = signal.signal

    def mock_signal(signalnum, handler):
        valid_signals = {
            signal.SIGINT,
            signal.SIGILL,
            signal.SIGFPE,
            signal.SIGSEGV,
            signal.SIGTERM,
            signal.SIGBREAK,
            signal.SIGABRT,
        }
        if signalnum in valid_signals:
            return orig_signal(signalnum, handler)
        return None

    signal.signal = mock_signal

# Add dags folder to sys.path so that callbacks and other local imports resolve during DagBag load
from pathlib import Path

DAGS_DIR = str(Path(__file__).resolve().parents[2] / "dags")
if DAGS_DIR not in sys.path:
    sys.path.insert(0, DAGS_DIR)

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session", autouse=True)
def mock_minio_client_for_integration():
    """Mock the MinIO client globally for integration tests to prevent connection attempts."""
    from unittest.mock import MagicMock, patch

    mock_client = MagicMock()
    with patch("src.utils.minio_client.create_minio_client", return_value=mock_client):
        yield mock_client


@pytest.fixture(scope="session", autouse=True)
def init_airflow_db(tmp_path_factory) -> None:
    """Initialize Airflow SQLite database for integration tests."""
    airflow_home = tmp_path_factory.mktemp("airflow_home")
    os.environ["AIRFLOW_HOME"] = str(airflow_home)
    os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = f"sqlite:///{airflow_home.as_posix()}/airflow.db"

    from airflow.utils.db import initdb

    initdb()


# Pin the Iceberg + Spark runtime coordinates that match the prod image
# (``docker/Dockerfile.spark`` bundles the same JAR locally; CI fetches it
# from Maven Central on first use, then the Ivy cache makes subsequent runs
# fast).
_ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0"


@pytest.fixture(scope="session")
def iceberg_warehouse(tmp_path_factory) -> Path:
    """Per-session local Iceberg warehouse root.

    Returned as a ``Path`` and re-used across every integration test so we
    don't pay Spark startup cost more than once. Cleared at session end.
    """
    root = tmp_path_factory.mktemp("warehouse")
    yield root
    # Best-effort teardown; failures here shouldn't mask test errors.
    shutil.rmtree(root, ignore_errors=True)


@pytest.fixture(scope="session")
def spark(iceberg_warehouse: Path) -> SparkSession:
    """SparkSession wired for Iceberg with a local catalog named ``nessie``.

    The catalog name **must** be ``nessie`` so the production SQL
    (``nessie.bronze.brasileirao`` etc.) runs unchanged. ``type=hadoop``
    means Spark talks to the local filesystem instead of a Nessie REST
    service.
    """
    session = (
        SparkSession.builder.master("local[2]")
        .appName("data_lake_integration_tests")
        .config("spark.jars.packages", _ICEBERG_PACKAGE)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.nessie.type", "hadoop")
        .config("spark.sql.catalog.nessie.warehouse", str(iceberg_warehouse))
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )
    yield session
    session.stop()
