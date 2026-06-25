"""Unit tests for the centralized Silver DDL module (F-02).

These mirror what the transform modules used to assert inline: the namespace is
always ensured, the table is created only on first run, and the Iceberg
properties (format-version 2 + gc.enabled) are present.
"""

from unittest.mock import MagicMock

import pytest

from src.db import ddl


@pytest.fixture
def spark():
    return MagicMock()


_ENSURE_FUNCS = [
    (ddl.ensure_silver_brasileirao, ddl.SILVER_BRASILEIRAO),
    (ddl.ensure_brasileirao_quarantine, ddl.BRASILEIRAO_QUARANTINE),
]


def _ddl_calls(spark) -> list[str]:
    return [args[0] for args, _ in spark.sql.call_args_list]


@pytest.mark.parametrize(("ensure", "table"), _ENSURE_FUNCS)
def test_creates_table_on_first_run(spark, ensure, table):
    spark.catalog.tableExists.return_value = False

    ensure(spark)

    calls = _ddl_calls(spark)
    assert any(c == "CREATE NAMESPACE IF NOT EXISTS nessie.silver" for c in calls)
    create = next((c for c in calls if "CREATE TABLE" in c), None)
    assert create is not None, "first run must CREATE TABLE"
    assert table in create
    assert "'format-version'='2'" in create
    assert "'gc.enabled'='true'" in create


@pytest.mark.parametrize(("ensure", "table"), _ENSURE_FUNCS)
def test_skips_create_when_table_exists(spark, ensure, table):
    spark.catalog.tableExists.return_value = True

    ensure(spark)

    calls = _ddl_calls(spark)
    assert any("CREATE NAMESPACE" in c for c in calls)
    assert not any("CREATE TABLE" in c for c in calls), "must not CREATE TABLE when it already exists"
