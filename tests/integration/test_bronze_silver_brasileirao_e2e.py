"""End-to-end integration tests: staging JSON → Bronze → Silver (Brasileirao).

Drives the real Brasileirao Bronze/Silver functions against a local Iceberg
warehouse (no Nessie, no MinIO — MinIO is mocked by the integration conftest).
Covers the contracts unit mocks can't: the path-driven Bronze ingestion, the
analytical column derivations, the stadium→UF enrichment cascade, the plain
UPSERT MERGE (no soft-delete), and the NULL-``ge_match_id`` quarantine split.
"""

from __future__ import annotations

import contextlib
import json
from typing import TYPE_CHECKING

import pytest

from src.bronze.ingest_brasileirao import _run_ingest
from src.silver.transform_brasileirao import _run_transform

if TYPE_CHECKING:
    from pathlib import Path

    from pyspark.sql import SparkSession

EXECUTION_DATE = "2026-05-30"


@pytest.fixture(autouse=True)
def reset_catalog(spark: SparkSession) -> None:
    """Drop the Brasileirao bronze/silver tables between tests for isolation."""
    yield
    for table in (
        "nessie.bronze.brasileirao",
        "nessie.silver.brasileirao",
        "nessie.silver.brasileirao_quarantine",
    ):
        with contextlib.suppress(Exception):
            spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")
    for namespace in ("bronze", "silver"):
        with contextlib.suppress(Exception):
            spark.sql(f"DROP NAMESPACE IF EXISTS nessie.{namespace}")


def _match(ge_id: str | None, *, home: str, away: str, sh: int, sa: int, stadium: str, week: int = 1) -> dict:
    """Build one staging match row matching BRASILEIRAO_SCHEMA."""
    return {
        "matchweek": week,
        "home_team": home,
        "home_team_code": home[:3].upper(),
        "away_team": away,
        "away_team_code": away[:3].upper(),
        "score_home": sh,
        "score_away": sa,
        "date": EXECUTION_DATE,
        "kickoff_time": "16:00",
        "stadium": stadium,
        "broadcast": "TV",
        "match_url": "http://ge.example/match",
        "match_started": True,
        "source": "ge",
        "ge_match_id": ge_id,
    }


def _write_staging(staging_root: Path, date: str, rows: list[dict]) -> str:
    """Write rows as ``<root>/staging/brasileirao/<date>/matches.json``.

    Returns the ``file://`` base whose ``/*/matches.json`` glob the Bronze
    ingest scans; ``ingestion_date`` is derived from the ``/brasileirao/<date>/``
    path segment, so the folder name must be the date.
    """
    base = staging_root / "staging" / "brasileirao"
    date_dir = base / date
    date_dir.mkdir(parents=True, exist_ok=True)
    (date_dir / "matches.json").write_text(json.dumps(rows), encoding="utf-8")
    return f"file://{base.as_posix()}"


def test_bronze_writes_partitioned_table(spark: SparkSession, tmp_path: Path) -> None:
    """Ingest derives ingestion_date from the path and keeps ge_match_id as a string."""
    rows = [
        _match("uuid-001", home="Flamengo", away="Palmeiras", sh=2, sa=0, stadium="Maracanã"),
        _match("uuid-002", home="Grêmio", away="Internacional", sh=1, sa=1, stadium="Arena do Grêmio"),
    ]
    base = _write_staging(tmp_path, EXECUTION_DATE, rows)

    _run_ingest(spark, EXECUTION_DATE, staging_path_base=base)

    out = spark.sql("SELECT ge_match_id, ingestion_date FROM nessie.bronze.brasileirao ORDER BY ge_match_id").collect()
    assert [r["ge_match_id"] for r in out] == ["uuid-001", "uuid-002"]
    assert all(r["ingestion_date"] == EXECUTION_DATE for r in out)


def test_silver_derives_total_goals_and_outcome(spark: SparkSession, tmp_path: Path) -> None:
    """Silver derives total_goals and match_outcome from the scoreline."""
    rows = [
        _match("home-win", home="Flamengo", away="Vasco", sh=3, sa=1, stadium="Maracanã"),
        _match("away-win", home="Santos", away="Corinthians", sh=0, sa=2, stadium="Vila Belmiro"),
        _match("draw", home="Bahia", away="Vitória", sh=1, sa=1, stadium="Arena Fonte Nova"),
    ]
    base = _write_staging(tmp_path, EXECUTION_DATE, rows)
    _run_ingest(spark, EXECUTION_DATE, staging_path_base=base)
    _run_transform(spark, EXECUTION_DATE)

    out = {
        r["ge_match_id"]: (r["total_goals"], r["match_outcome"])
        for r in spark.sql("SELECT ge_match_id, total_goals, match_outcome FROM nessie.silver.brasileirao").collect()
    }
    assert out == {
        "home-win": (4, "HOME_WIN"),
        "away-win": (2, "AWAY_WIN"),
        "draw": (2, "DRAW"),
    }


def test_silver_enriches_stadium_state_cascade(spark: SparkSession, tmp_path: Path) -> None:
    """The stadium → home_team → UNKNOWN enrichment cascade resolves correctly."""
    rows = [
        # 1) stadium hit: Maracanã → RJ
        _match("by-stadium", home="Flamengo", away="Vasco", sh=1, sa=0, stadium="Maracanã"),
        # 2) stadium miss, home_team fallback: unknown stadium + Palmeiras → SP
        _match("by-team", home="Palmeiras", away="Santos", sh=2, sa=0, stadium="Estadio Fantasma"),
        # 3) both miss → UNKNOWN sentinel
        _match("unknown", home="Time Inexistente", away="Outro", sh=0, sa=0, stadium="Estadio Fantasma"),
    ]
    base = _write_staging(tmp_path, EXECUTION_DATE, rows)
    _run_ingest(spark, EXECUTION_DATE, staging_path_base=base)
    _run_transform(spark, EXECUTION_DATE)

    out = {
        r["ge_match_id"]: (r["stadium_state"], r["stadium_state_origin"])
        for r in spark.sql(
            "SELECT ge_match_id, stadium_state, stadium_state_origin FROM nessie.silver.brasileirao"
        ).collect()
    }
    assert out == {
        "by-stadium": ("RJ", "STADIUM_LOOKUP"),
        "by-team": ("SP", "HOME_TEAM_FALLBACK"),
        "unknown": ("UNKNOWN", "UNKNOWN"),
    }


def test_quarantine_receives_null_ge_match_id(spark: SparkSession, tmp_path: Path) -> None:
    """NULL ge_match_id rows land in the quarantine sink, not the Silver table."""
    rows = [
        _match("ok-1", home="Flamengo", away="Vasco", sh=1, sa=0, stadium="Maracanã"),
        _match(None, home="Mystery FC", away="Nobody", sh=0, sa=0, stadium="Estadio Fantasma"),
    ]
    base = _write_staging(tmp_path, EXECUTION_DATE, rows)
    _run_ingest(spark, EXECUTION_DATE, staging_path_base=base)
    _run_transform(spark, EXECUTION_DATE)

    silver = spark.sql("SELECT COUNT(*) AS c FROM nessie.silver.brasileirao").collect()[0]["c"]
    quarantined = spark.sql("SELECT COUNT(*) AS c FROM nessie.silver.brasileirao_quarantine").collect()[0]["c"]
    assert silver == 1
    assert quarantined == 1

    reason = spark.sql("SELECT quarantine_reason FROM nessie.silver.brasileirao_quarantine LIMIT 1").collect()[0][
        "quarantine_reason"
    ]
    assert reason == "NULL_GE_MATCH_ID"


def test_silver_merge_is_upsert_no_soft_delete(spark: SparkSession, tmp_path: Path) -> None:
    """Re-processing a corrected scoreline updates the row in place (no dup, no soft-delete).

    The Brasileirao MERGE has no ``WHEN NOT MATCHED BY SOURCE``
    — a played match never disappears — so a second run with a single (updated)
    match must update that match and leave the absent one untouched/active.
    """
    first = [
        _match("match-1", home="Flamengo", away="Vasco", sh=1, sa=0, stadium="Maracanã"),
        _match("match-2", home="Santos", away="Corinthians", sh=0, sa=0, stadium="Vila Belmiro"),
    ]
    base = _write_staging(tmp_path, EXECUTION_DATE, first)
    _run_ingest(spark, EXECUTION_DATE, staging_path_base=base)
    _run_transform(spark, EXECUTION_DATE)

    # Correction: match-1 re-scored 3-2. overwritePartitions replaces the whole
    # date partition, so Bronze now holds only match-1; the full-reconcile Silver
    # MERGE updates it and — having no WHEN NOT MATCHED BY SOURCE — leaves the
    # already-loaded match-2 in place.
    corrected = [_match("match-1", home="Flamengo", away="Vasco", sh=3, sa=2, stadium="Maracanã")]
    base = _write_staging(tmp_path, EXECUTION_DATE, corrected)
    _run_ingest(spark, EXECUTION_DATE, staging_path_base=base)
    _run_transform(spark, EXECUTION_DATE)

    out = {
        r["ge_match_id"]: (r["total_goals"], r["match_outcome"], r["is_active"])
        for r in spark.sql(
            "SELECT ge_match_id, total_goals, match_outcome, is_active FROM nessie.silver.brasileirao"
        ).collect()
    }
    # match-1 updated in place; match-2 still present and active (no soft-delete).
    assert out["match-1"] == (5, "HOME_WIN", True)
    assert out["match-2"][2] is True
    assert len(out) == 2, "upsert must not duplicate rows"
