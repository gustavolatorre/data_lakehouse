"""Unit tests for ``src.silver.transform_brasileirao``.

Two complementary strategies:

* PySpark-fixture based tests for the small pure transformations
  (``_apply_native_transformations``, ``_enrich_with_stadium_state``) that
  benefit from real DataFrame semantics.
* Mock-based tests for the orchestration / SQL side (``_execute_merge``,
  ``_quarantine_invalid_records``, ``_run_transform``, ``transform``) so
  they don't need Iceberg / Nessie in the test classpath.
"""

from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import Row
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.silver import transform_brasileirao
from src.silver.transform_brasileirao import (
    BRONZE_TABLE,
    QUARANTINE_TABLE,
    REASON_NULL_ID,
    SILVER_TABLE,
    _apply_native_transformations,
    _enrich_with_stadium_state,
    _execute_merge,
    _quarantine_invalid_records,
    _run_transform,
    transform,
)

_BRONZE_SCHEMA = StructType(
    [
        StructField("ge_match_id", StringType(), True),
        StructField("matchweek", IntegerType(), True),
        StructField("home_team", StringType(), True),
        StructField("home_team_code", StringType(), True),
        StructField("away_team", StringType(), True),
        StructField("away_team_code", StringType(), True),
        StructField("score_home", IntegerType(), True),
        StructField("score_away", IntegerType(), True),
        StructField("date", StringType(), True),
        StructField("kickoff_time", StringType(), True),
        StructField("stadium", StringType(), True),
        StructField("broadcast", StringType(), True),
        StructField("match_url", StringType(), True),
        StructField("match_started", BooleanType(), True),
        StructField("source", StringType(), True),
        StructField("ingestion_date", StringType(), True),
        StructField("ingested_at", TimestampType(), True),
    ]
)


def _sample_bronze_rows():
    """Three matches covering the three enrichment cascade paths:

    * row 1 (Maracanã / Flamengo) → STADIUM_LOOKUP
    * row 2 (Estádio Imaginário / Cuiabá) → HOME_TEAM_FALLBACK (Cuiabá → MT)
    * row 3 (Estádio Imaginário / TimeXPTO) → UNKNOWN
    """
    import datetime

    ts = datetime.datetime(2026, 5, 25, 12, 0, 0)
    return [
        (
            "match-001",
            6,
            "Flamengo",
            "FLA",
            "Palmeiras",
            "PAL",
            2,
            1,
            "2026-05-25",
            "16:00",
            "Maracanã",
            "Premiere",
            "http://example.com",
            True,
            "ge.globo.com",
            "2026-05-25",
            ts,
        ),
        (
            "match-002",
            6,
            "Cuiabá",
            "CUI",
            "Goiás",
            "GOI",
            0,
            0,
            "2026-05-25",
            "18:30",
            "Estádio Imaginário",
            "Premiere",
            "http://example.com",
            True,
            "ge.globo.com",
            "2026-05-25",
            ts,
        ),
        (
            "match-003",
            6,
            "TimeXPTO",
            "XPT",
            "OutroTime",
            "OUT",
            1,
            3,
            "2026-05-25",
            "21:00",
            "Estádio Imaginário",
            "Premiere",
            "http://example.com",
            True,
            "ge.globo.com",
            "2026-05-25",
            ts,
        ),
    ]


@pytest.fixture
def bronze_df(spark):
    return spark.createDataFrame(_sample_bronze_rows(), schema=_BRONZE_SCHEMA)


@pytest.fixture
def patched_f():
    """Patch ``F`` so calls like F.col / F.lit / F.current_timestamp don't
    need a real SparkContext. Used by the mock-based orchestration tests.

    The Column mock returned by ``F.col`` is configured to support the
    comparison operators the production code uses (>=, isNotNull, etc.),
    because plain MagicMock raises TypeError on those.
    """
    with patch("src.silver.transform_brasileirao.F") as mock_f:
        col_mock = MagicMock(name="col_col")
        col_mock.__ge__ = MagicMock(return_value=MagicMock(name="col_ge"))
        col_mock.__gt__ = MagicMock(return_value=MagicMock(name="col_gt"))
        col_mock.isNotNull = MagicMock(return_value=MagicMock(name="col_isnotnull"))
        col_mock.isNull = MagicMock(return_value=MagicMock(name="col_isnull"))
        mock_f.col.return_value = col_mock
        mock_f.lit.return_value = MagicMock(name="lit_col")
        mock_f.current_timestamp.return_value = MagicMock(name="ts_col")
        yield mock_f


# ──────────────────────────────────────────────────────────────────────────
# _apply_native_transformations
# ──────────────────────────────────────────────────────────────────────────


class TestApplyNativeTransformations:
    def test_strips_accents_from_text_columns(self, spark):
        row = Row(
            ge_match_id="x",
            matchweek=1,
            home_team="São Paulo",
            home_team_code="SAO",
            away_team="Atlético-MG",
            away_team_code="CAM",
            score_home=1,
            score_away=1,
            date="2026-05-25",
            kickoff_time="16:00",
            stadium="Mineirão",
            broadcast="x",
            match_url="x",
            match_started=True,
            source="x",
            ingestion_date="2026-05-25",
            ingested_at="2026-05-25 12:00:00",
        )
        df = spark.createDataFrame([row])
        out = _apply_native_transformations(df).collect()[0]
        assert out.home_team == "Sao Paulo"
        assert out.away_team == "Atletico-MG"
        assert out.stadium == "Mineirao"

    def test_derives_match_outcome_home_win(self, bronze_df):
        # match-001: 2-1 Flamengo
        out = _apply_native_transformations(bronze_df).filter("ge_match_id = 'match-001'").collect()[0]
        assert out.match_outcome == "HOME_WIN"

    def test_derives_match_outcome_draw(self, bronze_df):
        # match-002: 0-0
        out = _apply_native_transformations(bronze_df).filter("ge_match_id = 'match-002'").collect()[0]
        assert out.match_outcome == "DRAW"

    def test_derives_match_outcome_away_win(self, bronze_df):
        # match-003: 1-3
        out = _apply_native_transformations(bronze_df).filter("ge_match_id = 'match-003'").collect()[0]
        assert out.match_outcome == "AWAY_WIN"

    def test_total_goals_is_sum(self, bronze_df):
        rows = {r.ge_match_id: r for r in _apply_native_transformations(bronze_df).collect()}
        assert rows["match-001"].total_goals == 3
        assert rows["match-002"].total_goals == 0
        assert rows["match-003"].total_goals == 4

    def test_match_date_is_date_type(self, bronze_df):
        import datetime

        out = _apply_native_transformations(bronze_df).select("match_date").collect()
        assert all(isinstance(r.match_date, datetime.date) for r in out)

    def test_kickoff_ts_combines_date_and_time(self, bronze_df):
        out = _apply_native_transformations(bronze_df).filter("ge_match_id = 'match-001'").collect()[0]
        # kickoff_time was "16:00" on 2026-05-25
        assert out.kickoff_ts is not None
        assert out.kickoff_ts.hour == 16
        assert out.kickoff_ts.minute == 0


# ──────────────────────────────────────────────────────────────────────────
# _enrich_with_stadium_state — cascade STADIUM_LOOKUP → HOME_TEAM_FALLBACK → UNKNOWN
# ──────────────────────────────────────────────────────────────────────────


class TestEnrichWithStadiumState:
    """End-to-end cascade tests on the transformed (accent-stripped) DataFrame.

    Both the Silver transform and ``build_lookup_frames`` normalize accents
    via the same translate map, so an accented dict key like ``"Maracanã"``
    materializes as ``"Maracana"`` and joins cleanly with the transformed
    column.
    """

    def test_stadium_lookup_path(self, spark, bronze_df):
        # match-001 stadium = "Maracanã" → after transform: "Maracana"
        # → STADIUM_TO_STATE (also normalized) has the same "Maracana" → "RJ"
        transformed = _apply_native_transformations(bronze_df)
        enriched = _enrich_with_stadium_state(spark, transformed)
        rows = {r.ge_match_id: r for r in enriched.collect()}

        assert rows["match-001"].stadium_state == "RJ"
        assert rows["match-001"].stadium_state_origin == "STADIUM_LOOKUP"

    def test_home_team_fallback_path(self, spark, bronze_df):
        # match-002 stadium = "Estádio Imaginário" → not in STADIUM_TO_STATE
        # → falls back to HOME_TEAM_TO_STATE lookup on "Cuiaba" (normalized
        # from "Cuiabá") → "MT"
        transformed = _apply_native_transformations(bronze_df)
        enriched = _enrich_with_stadium_state(spark, transformed)
        rows = {r.ge_match_id: r for r in enriched.collect()}

        assert rows["match-002"].stadium_state == "MT"
        assert rows["match-002"].stadium_state_origin == "HOME_TEAM_FALLBACK"

    def test_unknown_path(self, spark, bronze_df):
        # match-003 has neither a known stadium nor a known team → UNKNOWN
        transformed = _apply_native_transformations(bronze_df)
        enriched = _enrich_with_stadium_state(spark, transformed)
        rows = {r.ge_match_id: r for r in enriched.collect()}

        assert rows["match-003"].stadium_state == "UNKNOWN"
        assert rows["match-003"].stadium_state_origin == "UNKNOWN"

    def test_drops_lookup_helper_columns(self, spark, bronze_df):
        transformed = _apply_native_transformations(bronze_df)
        enriched = _enrich_with_stadium_state(spark, transformed)
        # Helper columns must not leak into the downstream schema.
        for col in ("_lookup_stadium", "_lookup_stadium_state", "_lookup_home_team", "_lookup_home_team_state"):
            assert col not in enriched.columns


# ──────────────────────────────────────────────────────────────────────────
# _execute_merge
# ──────────────────────────────────────────────────────────────────────────


class TestExecuteMerge:
    def test_creates_namespace(self):
        spark = MagicMock()
        spark.catalog.tableExists.return_value = True
        _execute_merge(spark)
        spark.sql.assert_any_call("CREATE NAMESPACE IF NOT EXISTS nessie.silver")

    def test_creates_table_on_first_run(self):
        spark = MagicMock()
        spark.catalog.tableExists.return_value = False
        _execute_merge(spark)
        # The CREATE TABLE call contains months(match_date) partitioning.
        ddl_calls = [c.args[0] for c in spark.sql.call_args_list if "CREATE TABLE" in c.args[0]]
        assert ddl_calls, "expected CREATE TABLE call"
        ddl = ddl_calls[0]
        assert "months(match_date)" in ddl
        assert SILVER_TABLE in ddl

    def test_skips_create_when_table_exists(self):
        spark = MagicMock()
        spark.catalog.tableExists.return_value = True
        _execute_merge(spark)
        ddl_calls = [c.args[0] for c in spark.sql.call_args_list if "CREATE TABLE" in c.args[0]]
        assert not ddl_calls

    def test_merge_has_no_when_not_matched_by_source(self):
        """Jogos historicos nao desaparecem da fonte — soft-delete nao se aplica."""
        spark = MagicMock()
        spark.catalog.tableExists.return_value = True
        _execute_merge(spark)
        merge_calls = [c.args[0] for c in spark.sql.call_args_list if "MERGE INTO" in c.args[0]]
        assert merge_calls, "expected a MERGE INTO call"
        sql = merge_calls[0]
        assert "WHEN MATCHED" in sql
        assert "WHEN NOT MATCHED THEN INSERT" in sql
        assert "WHEN NOT MATCHED BY SOURCE" not in sql

    def test_merge_keyed_on_ge_match_id(self):
        spark = MagicMock()
        spark.catalog.tableExists.return_value = True
        _execute_merge(spark)
        merge_sql = next(c.args[0] for c in spark.sql.call_args_list if "MERGE INTO" in c.args[0])
        assert "t.ge_match_id = s.ge_match_id" in merge_sql


# ──────────────────────────────────────────────────────────────────────────
# _quarantine_invalid_records
# ──────────────────────────────────────────────────────────────────────────


class TestQuarantine:
    def test_creates_quarantine_table_on_first_run(self, patched_f):
        spark = MagicMock()
        spark.catalog.tableExists.return_value = False
        bad_df = MagicMock()
        enriched = MagicMock()
        enriched.count.return_value = 1
        # Chain bad_df.select(...).withColumn().withColumn().withColumn()
        bad_df.select.return_value.withColumn.return_value.withColumn.return_value.withColumn.return_value = enriched

        _quarantine_invalid_records(spark, bad_df, REASON_NULL_ID, "2026-05-25")

        ddl_calls = [c.args[0] for c in spark.sql.call_args_list if "CREATE TABLE" in c.args[0]]
        assert ddl_calls, "expected CREATE TABLE call for quarantine"
        assert QUARANTINE_TABLE in ddl_calls[0]
        assert "quarantine_date" in ddl_calls[0]

    def test_skips_create_when_quarantine_table_exists(self, patched_f):
        spark = MagicMock()
        spark.catalog.tableExists.return_value = True
        bad_df = MagicMock()
        enriched = MagicMock()
        enriched.count.return_value = 1
        bad_df.select.return_value.withColumn.return_value.withColumn.return_value.withColumn.return_value = enriched

        _quarantine_invalid_records(spark, bad_df, REASON_NULL_ID, "2026-05-25")

        ddl_calls = [c.args[0] for c in spark.sql.call_args_list if "CREATE TABLE" in c.args[0]]
        assert not ddl_calls

    def test_appends_to_quarantine(self, patched_f):
        spark = MagicMock()
        spark.catalog.tableExists.return_value = True
        bad_df = MagicMock()
        enriched = MagicMock()
        enriched.count.return_value = 2
        bad_df.select.return_value.withColumn.return_value.withColumn.return_value.withColumn.return_value = enriched

        _quarantine_invalid_records(spark, bad_df, REASON_NULL_ID, "2026-05-25")

        enriched.writeTo.assert_called_once_with(QUARANTINE_TABLE)
        enriched.writeTo.return_value.append.assert_called_once()


# ──────────────────────────────────────────────────────────────────────────
# _run_transform — orchestration
# ──────────────────────────────────────────────────────────────────────────


class TestRunTransform:
    def test_short_circuits_when_bronze_missing(self):
        spark = MagicMock()
        spark.catalog.tableExists.return_value = False
        # Bronze table absent → exit before reading or any MERGE / quarantine.
        _run_transform(spark, "2026-05-25")
        spark.table.assert_not_called()
        spark.sql.assert_not_called()

    def test_short_circuits_when_bronze_empty(self):
        spark = MagicMock()
        spark.catalog.tableExists.return_value = True
        empty_df = MagicMock()
        empty_df.isEmpty.return_value = True
        spark.table.return_value = empty_df

        # Full reconcile reads the whole Bronze table; an empty table is a no-op.
        _run_transform(spark, "2026-05-25")
        spark.sql.assert_not_called()


# ──────────────────────────────────────────────────────────────────────────
# transform entrypoint
# ──────────────────────────────────────────────────────────────────────────


class TestTransformEntrypoint:
    @patch("src.silver.transform_brasileirao._run_transform")
    @patch("src.silver.transform_brasileirao.create_spark_session")
    def test_stops_spark_on_failure(self, mock_create, mock_run):
        mock_spark = MagicMock()
        mock_create.return_value = mock_spark
        mock_run.side_effect = RuntimeError("boom")

        with pytest.raises(RuntimeError, match="boom"):
            transform("2026-05-25")

        mock_spark.stop.assert_called_once()

    @patch("src.silver.transform_brasileirao._run_transform")
    @patch("src.silver.transform_brasileirao.create_spark_session")
    def test_happy_path_stops_spark(self, mock_create, mock_run):
        mock_spark = MagicMock()
        mock_create.return_value = mock_spark

        transform("2026-05-25")

        mock_run.assert_called_once()
        mock_spark.stop.assert_called_once()


class TestModuleConstants:
    def test_table_names(self):
        assert BRONZE_TABLE == "nessie.bronze.brasileirao"
        assert SILVER_TABLE == "nessie.silver.brasileirao"
        assert QUARANTINE_TABLE == "nessie.silver.brasileirao_quarantine"

    def test_reason_constant_stable(self):
        # Downstream alerting filters on this exact string.
        assert REASON_NULL_ID == "NULL_GE_MATCH_ID"

    def test_module_logger_named_after_module(self):
        assert transform_brasileirao.logger.name == "src.silver.transform_brasileirao"
