"""Shared Airflow task callbacks.

Centralizes the on_failure_callback used by all DAGs in this project so the
failure-log format stays consistent. Import from each DAG file:

    from callbacks import build_failure_callback

    @dag(default_args={"on_failure_callback": build_failure_callback("STAGING")})
"""

import logging
from collections.abc import Callable

logger = logging.getLogger("airflow.task")


def build_failure_callback(layer: str) -> Callable[[dict], None]:
    """Build an on_failure_callback that tags log lines with the given layer name.

    Args:
        layer: Human-readable label inserted into the log line (e.g. "STAGING",
            "BRONZE/SILVER", "GOLD DBT"). Surfaces in the Airflow task logs and
            downstream alerting.

    Returns:
        A callback function compatible with Airflow's default_args.on_failure_callback
        signature: ``Callable[[dict], None]``.
    """

    def _callback(context: dict) -> None:
        task_instance = context.get("task_instance")
        dag = context.get("dag")
        dag_id = dag.dag_id if dag else "unknown"
        task_id = task_instance.task_id if task_instance else "unknown"
        execution_date = context.get("ds", "unknown")
        exception = context.get("exception", "No exception info")

        logger.error(
            "%s FAILURE | dag=%s | task=%s | date=%s | error=%s",
            layer,
            dag_id,
            task_id,
            execution_date,
            exception,
        )

        # F-17: deliver the failure to the configured alert sink (log-only by
        # default). Imported lazily so DAG parsing stays light.
        from src.utils.alerting import send_alert

        send_alert(
            f"{layer} task failed",
            f"dag={dag_id} task={task_id} date={execution_date} error={exception}",
            severity="error",
        )

    return _callback
