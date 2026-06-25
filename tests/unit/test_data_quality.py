"""Unit tests for data quality check functions."""

import pytest

from src.utils.data_quality import check_null_counts, check_row_count, log_quality_summary


class TestCheckNullCounts:
    """Tests for the check_null_counts function."""

    def test_detects_nulls(self, sample_df):
        """Should detect null values in specified columns."""
        results = check_null_counts(sample_df, ["stadium_state", "broadcast"])

        assert results["stadium_state"] == 1  # ge-003 has null stadium_state
        assert results["broadcast"] >= 1  # ge-003 and ge-004 have null broadcast

    def test_no_nulls_in_id(self, sample_df):
        """Should report zero nulls for the ge_match_id column."""
        results = check_null_counts(sample_df, ["ge_match_id"])
        assert results["ge_match_id"] == 0

    def test_fail_on_nulls_raises(self, sample_df):
        """Should raise ValueError when fail_on_nulls is True and nulls exist."""
        with pytest.raises(ValueError, match="null values"):
            check_null_counts(sample_df, ["stadium_state"], fail_on_nulls=True)

    def test_skips_missing_columns(self, sample_df):
        """Should skip columns that don't exist in the DataFrame."""
        results = check_null_counts(sample_df, ["nonexistent_column"])
        assert "nonexistent_column" not in results


class TestCheckRowCount:
    """Tests for the check_row_count function."""

    def test_sufficient_rows_passes(self, sample_df):
        """Should return count when DataFrame meets minimum."""
        count = check_row_count(sample_df, min_rows=1)
        assert count == 5

    def test_empty_df_raises(self, empty_df):
        """Should raise ValueError when DataFrame is empty."""
        with pytest.raises(ValueError, match="expected at least"):
            check_row_count(empty_df, min_rows=1)

    def test_threshold_exceeded_raises(self, sample_df):
        """Should raise ValueError when count is below min_rows."""
        with pytest.raises(ValueError, match="expected at least"):
            check_row_count(sample_df, min_rows=100)


class TestLogQualitySummary:
    """Tests for the log_quality_summary function."""

    def test_returns_summary_dict(self, sample_df):
        """Should return a dict with row_count and column_count."""
        summary = log_quality_summary(sample_df, "test")

        assert summary["row_count"] == 5
        assert summary["column_count"] == 8

    def test_includes_null_counts_when_specified(self, sample_df):
        """Should include null_counts when critical_columns are provided."""
        summary = log_quality_summary(sample_df, "test", critical_columns=["ge_match_id", "stadium_state"])

        assert "null_counts" in summary
        assert summary["null_counts"]["ge_match_id"] == 0
        assert summary["null_counts"]["stadium_state"] == 1
