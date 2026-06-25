"""Unit tests for ``src.utils.quality_runner``.

The runner gathers every metric in one ``_collect_metrics`` Spark pass and then
evaluates each rule from that snapshot. These tests patch ``_collect_metrics``
to feed controlled numbers, so they exercise the dispatch / severity / YAML
logic without Spark. The real single-pass aggregation is exercised end-to-end
by the integration tests (``_run_transform`` → ``run_quality_checks`` against a
live Iceberg table).
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.utils.quality_runner import (
    SEVERITY_WARN,
    QualityCheckError,
    _Metrics,
    run_quality_checks,
)

_COLLECT = "src.utils.quality_runner._collect_metrics"


def _write_yaml(directory: Path, name: str, content: str) -> None:
    (directory / name).write_text(content, encoding="utf-8")


def _metrics(
    total: int = 10,
    null: dict[str, int] | None = None,
    nonnull: dict[str, int] | None = None,
    distinct: dict[str, int] | None = None,
) -> _Metrics:
    """Build a controlled metrics snapshot to stand in for the Spark agg pass."""
    return _Metrics(
        total_rows=total,
        null_count=null or {},
        nonnull_count=nonnull or {},
        distinct_count=distinct or {},
    )


@pytest.fixture
def checks_dir(tmp_path: Path) -> Path:
    """Temp directory the runner reads YAML rule files from."""
    return tmp_path


# ---------------------------------------------------------------------------
# YAML loading (fails before metrics are ever collected)
# ---------------------------------------------------------------------------


class TestYamlLoading:
    def test_missing_file_raises(self, checks_dir: Path):
        with pytest.raises(FileNotFoundError, match="not found"):
            run_quality_checks(MagicMock(), "does_not_exist.yml", checks_dir=checks_dir)

    def test_invalid_yaml_structure_raises(self, checks_dir: Path):
        _write_yaml(checks_dir, "bad.yml", "this_is_not_checks: [a, b]")
        with pytest.raises(ValueError, match="expected a top-level 'checks' list"):
            run_quality_checks(MagicMock(), "bad.yml", checks_dir=checks_dir)

    def test_checks_must_be_a_list(self, checks_dir: Path):
        _write_yaml(checks_dir, "bad.yml", "checks: not_a_list")
        with pytest.raises(ValueError, match="must be a list"):
            run_quality_checks(MagicMock(), "bad.yml", checks_dir=checks_dir)


# ---------------------------------------------------------------------------
# Row count
# ---------------------------------------------------------------------------


class TestRowCountRule:
    @patch(_COLLECT)
    def test_passes_when_above_min(self, mock_collect, checks_dir: Path):
        _write_yaml(
            checks_dir, "r.yml", "dataset: t\nchecks:\n  - name: must_have_rows\n    type: row_count\n    min: 1\n"
        )
        mock_collect.return_value = _metrics(total=5)

        results = run_quality_checks(MagicMock(), "r.yml", checks_dir=checks_dir)

        assert len(results) == 1
        assert results[0].passed
        assert results[0].actual == 5

    @patch(_COLLECT)
    def test_fails_when_below_min(self, mock_collect, checks_dir: Path):
        _write_yaml(
            checks_dir, "r.yml", "dataset: t\nchecks:\n  - name: must_have_rows\n    type: row_count\n    min: 10\n"
        )
        mock_collect.return_value = _metrics(total=5)

        with pytest.raises(QualityCheckError, match="must_have_rows"):
            run_quality_checks(MagicMock(), "r.yml", checks_dir=checks_dir)


# ---------------------------------------------------------------------------
# Missing count
# ---------------------------------------------------------------------------


class TestMissingCountRule:
    @patch(_COLLECT)
    def test_passes_when_zero_nulls(self, mock_collect, checks_dir: Path):
        _write_yaml(
            checks_dir,
            "r.yml",
            "dataset: t\nchecks:\n  - name: id_no_nulls\n    type: missing_count\n    column: id\n    max: 0\n",
        )
        mock_collect.return_value = _metrics(total=5, null={"id": 0})

        results = run_quality_checks(MagicMock(), "r.yml", checks_dir=checks_dir)

        assert results[0].passed
        assert results[0].actual == 0

    @patch(_COLLECT)
    def test_fails_when_nulls_present(self, mock_collect, checks_dir: Path):
        _write_yaml(
            checks_dir,
            "r.yml",
            "dataset: t\nchecks:\n  - name: id_no_nulls\n    type: missing_count\n    column: id\n    max: 0\n",
        )
        mock_collect.return_value = _metrics(total=5, null={"id": 2})

        with pytest.raises(QualityCheckError, match="id_no_nulls"):
            run_quality_checks(MagicMock(), "r.yml", checks_dir=checks_dir)


# ---------------------------------------------------------------------------
# Unique count
# ---------------------------------------------------------------------------


class TestUniqueCountRule:
    @patch(_COLLECT)
    def test_passes_when_all_unique(self, mock_collect, checks_dir: Path):
        _write_yaml(
            checks_dir, "r.yml", "dataset: t\nchecks:\n  - name: id_unique\n    type: unique_count\n    column: id\n"
        )
        mock_collect.return_value = _metrics(total=5, nonnull={"id": 5}, distinct={"id": 5})

        results = run_quality_checks(MagicMock(), "r.yml", checks_dir=checks_dir)

        assert results[0].passed
        assert results[0].actual == 0  # zero duplicates

    @patch(_COLLECT)
    def test_fails_on_duplicates(self, mock_collect, checks_dir: Path):
        _write_yaml(
            checks_dir, "r.yml", "dataset: t\nchecks:\n  - name: id_unique\n    type: unique_count\n    column: id\n"
        )
        # 5 non-null rows, only 3 distinct → 2 duplicates.
        mock_collect.return_value = _metrics(total=5, nonnull={"id": 5}, distinct={"id": 3})

        with pytest.raises(QualityCheckError, match="id_unique"):
            run_quality_checks(MagicMock(), "r.yml", checks_dir=checks_dir)


# ---------------------------------------------------------------------------
# Missing percent
# ---------------------------------------------------------------------------


class TestMissingPercentRule:
    @patch(_COLLECT)
    def test_warn_severity_does_not_raise(self, mock_collect, checks_dir: Path):
        """A `warn` violation must NOT raise, even when the rule failed."""
        _write_yaml(
            checks_dir,
            "r.yml",
            (
                "dataset: t\n"
                "checks:\n"
                "  - name: name_rarely_missing\n"
                "    type: missing_percent\n"
                "    column: name\n"
                "    max_percent: 1.0\n"
                "    severity: warn\n"
            ),
        )
        # 10% missing on a 1% threshold — would FAIL if severity=fail.
        mock_collect.return_value = _metrics(total=100, null={"name": 10})

        results = run_quality_checks(MagicMock(), "r.yml", checks_dir=checks_dir)

        assert not results[0].passed
        assert results[0].severity == SEVERITY_WARN
        assert results[0].actual == 10.0  # no raise — warn does not abort

    @patch(_COLLECT)
    def test_empty_df_passes(self, mock_collect, checks_dir: Path):
        _write_yaml(
            checks_dir,
            "r.yml",
            "dataset: t\nchecks:\n  - name: any_missing\n    type: missing_percent\n    column: name\n    max_percent: 0\n",
        )
        mock_collect.return_value = _metrics(total=0)

        results = run_quality_checks(MagicMock(), "r.yml", checks_dir=checks_dir)

        assert results[0].passed  # trivially passes on empty
        assert results[0].actual == 0.0


# ---------------------------------------------------------------------------
# Dispatch + required fields
# ---------------------------------------------------------------------------


class TestRuleDispatch:
    @patch(_COLLECT)
    def test_unknown_rule_type_is_treated_as_fail(self, mock_collect, checks_dir: Path):
        _write_yaml(checks_dir, "r.yml", "dataset: t\nchecks:\n  - name: bogus\n    type: not_a_real_rule\n")
        mock_collect.return_value = _metrics()

        # Default severity = fail; an unknown rule type means we couldn't
        # validate, so the runner returns a failed CheckResult AND raises.
        with pytest.raises(QualityCheckError):
            run_quality_checks(MagicMock(), "r.yml", checks_dir=checks_dir)

    @patch(_COLLECT)
    def test_missing_column_field_raises_value_error(self, mock_collect, checks_dir: Path):
        _write_yaml(checks_dir, "r.yml", "dataset: t\nchecks:\n  - name: bad\n    type: missing_count\n    max: 0\n")
        mock_collect.return_value = _metrics()

        with pytest.raises(ValueError, match="missing required field 'column'"):
            run_quality_checks(MagicMock(), "r.yml", checks_dir=checks_dir)


# ---------------------------------------------------------------------------
# Bundled rule file shape (sanity / freeze)
# ---------------------------------------------------------------------------


class TestBundledChecksFile:
    """Guarantees on the YAML we ship under quality/checks/."""

    def test_bronze_brasileirao_yaml_is_loadable_and_has_required_keys(self):
        import yaml

        bundled = Path(__file__).resolve().parents[2] / "quality" / "checks" / "bronze_brasileirao.yml"
        assert bundled.exists(), "shipped bronze rules file should exist"

        data = yaml.safe_load(bundled.read_text(encoding="utf-8"))
        assert data["dataset"] == "bronze.brasileirao"
        assert isinstance(data["checks"], list)
        assert len(data["checks"]) >= 1

        # Stable names — downstream alerting filters on these.
        names = {r["name"] for r in data["checks"]}
        assert "bronze_brasileirao_must_not_be_empty" in names
