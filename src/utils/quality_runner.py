"""Declarative quality-check runner for Spark DataFrames (P3.7).

Reads a YAML rule file under ``quality/checks/`` and executes the whole rule
set against the given DataFrame in **one** Spark aggregation pass: every metric
the rules need (the row count, per-column NULL counts, per-column non-null and
distinct counts) is gathered in a single ``df.agg(...).collect()``, then each
rule is evaluated in pure Python from those numbers. So N rules cost one Spark
job, not ~2N table scans. Logs PASS/WARN/FAIL per rule and raises
:class:`QualityCheckError` if any ``fail``-severity rule is violated.

This is intentionally a small, focused runner — not a full Great Expectations /
Soda integration. It scratches the same itch (declarative rules, outside the
pipeline's Python code, with stable names) with zero external deps beyond PyYAML.

Rule schema (per item in ``checks``):

* ``name`` (str, required): stable identifier surfaced in logs.
* ``type``: one of:
    - ``row_count``         : table-level, expects ``min``
    - ``missing_count``     : column-level, expects ``column`` + ``max``
    - ``unique_count``      : column-level, expects ``column``
    - ``missing_percent``   : column-level, expects ``column`` + ``max_percent``
* ``column`` (str, optional)
* ``min`` / ``max`` (int, optional)
* ``max_percent`` (float, optional)
* ``severity``: ``fail`` (default) or ``warn``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

# Dynamically resolve quality checks directory relative to codebase root.
# Works in production container (/opt/airflow/quality/checks), local env, and CI.
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CHECKS_DIR = _PROJECT_ROOT / "quality" / "checks"

# Severity tags used in YAML.
SEVERITY_FAIL = "fail"
SEVERITY_WARN = "warn"

# Rule types that need a per-column NULL count (missing_count + missing_percent).
_NULL_METRIC_TYPES = ("missing_count", "missing_percent")


class QualityCheckError(RuntimeError):
    """Raised when at least one ``fail``-severity rule violated."""


@dataclass(frozen=True)
class CheckResult:
    """Outcome of a single rule."""

    name: str
    rule_type: str
    severity: str
    passed: bool
    actual: Any
    expected: str
    message: str


@dataclass(frozen=True)
class _Metrics:
    """Every aggregate the rule set needs, gathered in a single Spark pass."""

    total_rows: int
    null_count: dict[str, int] = field(default_factory=dict)
    nonnull_count: dict[str, int] = field(default_factory=dict)
    distinct_count: dict[str, int] = field(default_factory=dict)


def run_quality_checks(
    df: DataFrame,
    checks_file: str,
    *,
    checks_dir: Path = DEFAULT_CHECKS_DIR,
) -> list[CheckResult]:
    """Execute every rule in ``checks_file`` against ``df``.

    Args:
        df: DataFrame to validate.
        checks_file: Filename inside ``checks_dir`` (e.g. ``bronze_brasileirao.yml``).
        checks_dir: Directory containing the YAML rules. Defaults to
            ``/opt/airflow/quality/checks`` (mirrors the docker-compose mount).

    Returns:
        List of CheckResult, one per rule.

    Raises:
        QualityCheckError: If any rule with ``severity: fail`` did not pass.
        FileNotFoundError: If the YAML file does not exist.
        ValueError: If the YAML is structurally invalid.
    """
    config = _load_checks(checks_dir / checks_file)
    dataset = config.get("dataset", "unknown")
    rules = config.get("checks", [])

    logger.info("Running %d quality check(s) on dataset '%s'", len(rules), dataset)

    metrics = _collect_metrics(df, rules)
    results = [_evaluate_rule(rule, metrics) for rule in rules]
    _log_results(results, dataset)

    failed = [r for r in results if r.severity == SEVERITY_FAIL and not r.passed]
    if failed:
        names = ", ".join(r.name for r in failed)
        raise QualityCheckError(f"Quality checks failed on '{dataset}': {names}")

    return results


def _load_checks(path: Path) -> dict[str, Any]:
    """Parse and validate the YAML rule file."""
    if not path.exists():
        msg = f"Quality checks file not found: {path}"
        raise FileNotFoundError(msg)

    with path.open(encoding="utf-8") as fp:
        data = yaml.safe_load(fp)

    if not isinstance(data, dict) or "checks" not in data:
        msg = f"Invalid quality checks file '{path}': expected a top-level 'checks' list"
        raise ValueError(msg)

    if not isinstance(data["checks"], list):
        msg = f"Invalid quality checks file '{path}': 'checks' must be a list"
        raise ValueError(msg)

    return data


def _collect_metrics(df: DataFrame, rules: list[dict[str, Any]]) -> _Metrics:
    """Gather every metric the rule set needs in a single ``df.agg(...)`` pass.

    Walks the rules to decide which per-column aggregates are required, builds
    them into one aggregation, and runs a single ``collect()``. The result is a
    plain-Python :class:`_Metrics` snapshot the evaluators read from — so adding
    rules never adds table scans.
    """
    null_cols: set[str] = set()
    unique_cols: set[str] = set()
    for rule in rules:
        column = rule.get("column")
        if not column:
            continue
        rule_type = rule.get("type")
        if rule_type in _NULL_METRIC_TYPES:
            null_cols.add(column)
        elif rule_type == "unique_count":
            unique_cols.add(column)

    null_alias = {c: f"null_{i}" for i, c in enumerate(sorted(null_cols))}
    nonnull_alias = {c: f"nn_{i}" for i, c in enumerate(sorted(unique_cols))}
    distinct_alias = {c: f"dc_{i}" for i, c in enumerate(sorted(unique_cols))}

    exprs = [F.count(F.lit(1)).alias("total")]
    for column, alias in null_alias.items():
        exprs.append(F.sum(F.col(column).isNull().cast("long")).alias(alias))
    for column in sorted(unique_cols):
        exprs.append(F.count(F.col(column)).alias(nonnull_alias[column]))
        exprs.append(F.countDistinct(F.col(column)).alias(distinct_alias[column]))

    row = df.agg(*exprs).collect()[0]

    return _Metrics(
        total_rows=int(row["total"] or 0),
        null_count={c: int(row[a] or 0) for c, a in null_alias.items()},
        nonnull_count={c: int(row[nonnull_alias[c]] or 0) for c in unique_cols},
        distinct_count={c: int(row[distinct_alias[c]] or 0) for c in unique_cols},
    )


def _evaluate_rule(rule: dict[str, Any], metrics: _Metrics) -> CheckResult:
    """Dispatch a single rule to the matching evaluator (pure — no Spark)."""
    name = rule.get("name", "<unnamed>")
    rule_type = rule.get("type", "")
    severity = rule.get("severity", SEVERITY_FAIL)

    evaluators = {
        "row_count": _check_row_count,
        "missing_count": _check_missing_count,
        "unique_count": _check_unique_count,
        "missing_percent": _check_missing_percent,
    }

    evaluator = evaluators.get(rule_type)
    if evaluator is None:
        return CheckResult(
            name=name,
            rule_type=rule_type,
            severity=severity,
            passed=False,
            actual="n/a",
            expected="n/a",
            message=f"unknown rule type '{rule_type}'",
        )

    return evaluator(rule, name, severity, metrics)


def _check_row_count(rule: dict[str, Any], name: str, severity: str, metrics: _Metrics) -> CheckResult:
    """Table-level: row count must be >= ``min``."""
    minimum = int(rule.get("min", 1))
    actual = metrics.total_rows
    passed = actual >= minimum
    return CheckResult(
        name=name,
        rule_type="row_count",
        severity=severity,
        passed=passed,
        actual=actual,
        expected=f">= {minimum}",
        message=f"row_count={actual} (expected >= {minimum})",
    )


def _check_missing_count(rule: dict[str, Any], name: str, severity: str, metrics: _Metrics) -> CheckResult:
    """Column-level: NULL count must be <= ``max`` (default 0)."""
    column = _required(rule, "column", name)
    maximum = int(rule.get("max", 0))
    actual = metrics.null_count[column]
    passed = actual <= maximum
    return CheckResult(
        name=name,
        rule_type="missing_count",
        severity=severity,
        passed=passed,
        actual=actual,
        expected=f"<= {maximum}",
        message=f"missing_count({column})={actual} (expected <= {maximum})",
    )


def _check_unique_count(rule: dict[str, Any], name: str, severity: str, metrics: _Metrics) -> CheckResult:
    """Column-level: every non-null value must be unique (zero duplicates)."""
    column = _required(rule, "column", name)
    total = metrics.nonnull_count[column]
    distinct = metrics.distinct_count[column]
    duplicates = total - distinct
    passed = duplicates == 0
    return CheckResult(
        name=name,
        rule_type="unique_count",
        severity=severity,
        passed=passed,
        actual=duplicates,
        expected="0",
        message=f"unique_count({column}): {duplicates} duplicate(s) of {total} non-null rows",
    )


def _check_missing_percent(rule: dict[str, Any], name: str, severity: str, metrics: _Metrics) -> CheckResult:
    """Column-level: NULL percentage must be <= ``max_percent``."""
    column = _required(rule, "column", name)
    max_percent = float(rule.get("max_percent", 0.0))
    total = metrics.total_rows
    if total == 0:
        return CheckResult(
            name=name,
            rule_type="missing_percent",
            severity=severity,
            passed=True,
            actual=0.0,
            expected=f"<= {max_percent}%",
            message=f"missing_percent({column}): trivially 0 (empty DataFrame)",
        )
    missing = metrics.null_count[column]
    pct = (missing / total) * 100.0
    passed = pct <= max_percent
    return CheckResult(
        name=name,
        rule_type="missing_percent",
        severity=severity,
        passed=passed,
        actual=round(pct, 2),
        expected=f"<= {max_percent}%",
        message=f"missing_percent({column})={pct:.2f}% (expected <= {max_percent}%)",
    )


def _required(rule: dict[str, Any], key: str, name: str) -> str:
    """Pull a required field out of a rule dict or raise with a clear message."""
    value = rule.get(key)
    if value is None:
        msg = f"Rule '{name}' is missing required field '{key}'"
        raise ValueError(msg)
    return str(value)


def _log_results(results: list[CheckResult], dataset: str) -> None:
    """Emit one log line per rule + a closing summary."""
    pass_count = sum(1 for r in results if r.passed)
    warn_count = sum(1 for r in results if not r.passed and r.severity == SEVERITY_WARN)
    fail_count = sum(1 for r in results if not r.passed and r.severity == SEVERITY_FAIL)

    for r in results:
        if r.passed:
            logger.info("[QC %s] PASS %s: %s", dataset, r.name, r.message)
        elif r.severity == SEVERITY_WARN:
            logger.warning("[QC %s] WARN %s: %s", dataset, r.name, r.message)
        else:
            logger.error("[QC %s] FAIL %s: %s", dataset, r.name, r.message)

    logger.info(
        "[QC %s] Summary: pass=%d warn=%d fail=%d (total=%d)",
        dataset,
        pass_count,
        warn_count,
        fail_count,
        len(results),
    )
