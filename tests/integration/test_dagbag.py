"""DagBag-level validation: every DAG file loads and registers cleanly.

Unit tests in ``test_dags.py`` parse the DAG files as Python AST and
check the structural contract without ever importing Airflow. That's
useful but it can't catch:
* import errors against the installed Airflow version,
* a typo in an asset URI that only blows up at load time,
* a callable referenced in ``default_args`` that doesn't exist,
* trigger_rule strings that resolve to nothing in the current Airflow
  release.

So this file does what the unit tests deliberately avoid: it spins up the
real DagBag against ``dags/``. It's skipped if Airflow isn't installed
(the unit-test Test job doesn't pull airflow extras; the Integration job
does).
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

# Mock fcntl for Windows environment to allow Airflow import
if sys.platform == "win32":
    sys.modules["fcntl"] = MagicMock()

import pytest

# Skip the whole module if Airflow isn't on the path. Keeps the file safe
# to discover under the lean unit-test job, which doesn't install the
# ``airflow`` extra.
airflow = pytest.importorskip("airflow")  # noqa: F841
from airflow.dag_processing.dagbag import DagBag  # noqa: E402

DAGS_DIR = Path(__file__).resolve().parents[2] / "dags"

EXPECTED_DAG_IDS = {
    "iceberg_maintenance",
    "staging_brasileirao_ingestion",
    "bronze_silver_brasileirao_processing",
    "gold_dbt_brasileirao_processing",
}


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    """Load every DAG in ``dags/`` once per module."""
    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)


def test_dagbag_has_no_import_errors(dagbag: DagBag) -> None:
    """Any import error fails the load — surface them eagerly here.

    ``DagBag`` collects errors silently and lets you `.get_dag()` to None
    when something didn't parse. The error string is short but identifies
    file + cause; print verbatim so the CI log is enough to diagnose.
    """
    assert not dagbag.import_errors, "DagBag failed to import:\n" + "\n".join(
        f"{p}: {e}" for p, e in dagbag.import_errors.items()
    )


def test_every_expected_dag_is_registered(dagbag: DagBag) -> None:
    """The 4 production DAGs are all present.

    Drops here mean either a DAG file was deleted, the ``dag_id`` was
    renamed without updating this test, or the file errored during load
    (cross-check ``test_dagbag_has_no_import_errors``).
    """
    actual = set(dagbag.dag_ids)
    missing = EXPECTED_DAG_IDS - actual
    assert not missing, f"missing dags: {missing}; got: {actual}"


@pytest.mark.parametrize("dag_id", sorted(EXPECTED_DAG_IDS))
def test_dag_has_tasks_and_no_cycles(dagbag: DagBag, dag_id: str) -> None:
    """Each DAG has tasks and the topology is acyclic.

    ``DagBag`` calls ``DAG.check_cycle()`` on load, but only at parse time
    of the decorator. An explicit per-DAG check pins the contract here too.
    """
    dag = dagbag.get_dag(dag_id)
    assert dag is not None, f"{dag_id} did not register"
    assert len(dag.tasks) > 0, f"{dag_id} has no tasks"
    # In Airflow 3 ``check_cycle`` raises rather than returns bool when a
    # cycle exists; presence-only check is enough here.
    dag.check_cycle()
