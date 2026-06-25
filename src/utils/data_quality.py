"""Imperative data-quality helpers for PySpark DataFrames.

These are the lightweight inline checks the Bronze/Silver jobs call directly
(``check_row_count``, ``log_quality_summary``) for logging and fail-fast
guards. They are the imperative complement to :mod:`src.utils.quality_runner`,
which evaluates the *declarative* YAML rule contracts under ``quality/checks/``.

Rule of thumb: put new reusable rules in the declarative runner; keep only
ad-hoc inline assertions here.
"""

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def check_null_counts(
    df: DataFrame,
    columns: list[str],
    fail_on_nulls: bool = False,
) -> dict[str, int]:
    """Check null value counts in specified columns.

    Args:
        df: Input DataFrame to check.
        columns: List of column names to inspect for nulls.
        fail_on_nulls: If True, raises ValueError when nulls are found.

    Returns:
        Dictionary mapping column names to their null counts.

    Raises:
        ValueError: If fail_on_nulls is True and any column has null values.
    """
    # Skip columns that don't exist on the DataFrame (warn once each).
    valid_columns: list[str] = []
    for col_name in columns:
        if col_name not in df.columns:
            logger.warning("Column '%s' not found in DataFrame, skipping null check", col_name)
            continue
        valid_columns.append(col_name)

    if not valid_columns:
        return {}

    # Single aggregation: count nulls for every column in one Spark action,
    # avoiding the previous N-actions-per-call pattern that triggered a full
    # scan per column (very expensive on Iceberg).
    null_row = df.agg(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in valid_columns]).collect()[0]

    results: dict[str, int] = {c: int(null_row[c] or 0) for c in valid_columns}

    # Emit warnings (and optionally raise) for every column that has any nulls.
    nonzero = {c: n for c, n in results.items() if n > 0}
    if nonzero and fail_on_nulls:
        msg = "null values found in columns: " + ", ".join(f"{c}={n}" for c, n in nonzero.items())
        logger.error(msg)
        raise ValueError(msg)
    for col_name, null_count in nonzero.items():
        logger.warning("%d null values found in column '%s'", null_count, col_name)

    return results


def check_row_count(
    df: DataFrame,
    min_rows: int = 1,
) -> int:
    """Validate that a DataFrame has a minimum number of rows.

    Args:
        df: Input DataFrame to check.
        min_rows: Minimum number of rows expected.

    Returns:
        Actual row count.

    Raises:
        ValueError: If the DataFrame has fewer rows than min_rows.
    """
    count = df.count()

    if count < min_rows:
        msg = f"DataFrame has {count} rows, expected at least {min_rows}"
        logger.error(msg)
        raise ValueError(msg)

    logger.info("Row count check passed: %d rows (minimum: %d)", count, min_rows)
    return count


def log_quality_summary(
    df: DataFrame,
    layer: str,
    critical_columns: list[str] | None = None,
) -> dict[str, int | dict[str, int]]:
    """Log a comprehensive data quality summary for a DataFrame.

    Args:
        df: Input DataFrame to summarize.
        layer: Layer name for logging context (e.g., 'bronze', 'silver', 'gold').
        critical_columns: Columns to check for nulls (optional).

    Returns:
        Dictionary with quality metrics: row_count, column_count, null_counts.
    """
    row_count = df.count()
    col_count = len(df.columns)

    summary: dict[str, int | dict[str, int]] = {
        "row_count": row_count,
        "column_count": col_count,
    }

    logger.info(
        "[%s] Quality Summary — rows: %d, columns: %d",
        layer.upper(),
        row_count,
        col_count,
    )

    if critical_columns:
        null_counts = check_null_counts(df, critical_columns)
        summary["null_counts"] = null_counts

    return summary
