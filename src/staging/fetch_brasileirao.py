"""Staging layer — fetch Brasileirão Série A data from GE (Globo) internal API.

This module fetches match data from the GE's internal JSON endpoint
(the same one consumed by the browser), extracts match information
(home/away teams, score, date, stadium, broadcast), and stores raw JSON
files in the MinIO staging bucket, partitioned by date.

**Current-season auto-derivation.** The Brasileirão championship UUID is
*stable* across editions; only the phase slug changes per year, deterministically
(``fase-unica-campeonato-brasileiro-{year}``). So the active edition is derived
from the run's year (``execution_date``) — the pipeline rolls over to the next
season automatically, with no per-year config edit. ``GE_SEASONS`` (CSV of years)
is an optional override to pin/force specific edition(s). The GE public API only
serves the *active* season; past editions return errors. Seasons separate
downstream naturally because staging is partitioned by *match date*, the Silver
MERGE upserts on the (globally unique) ``ge_match_id``, and the Gold derives the
season from the match-date year.

Contract (**full reconcile**): for every selected season, every run fetches ALL
rounds (1-38) and (re)stages EVERY finished match up to ``execution_date``, one
file per match date (``brasileirao/YYYY-MM-DD/matches.json``), overwriting
idempotently. There is no "today-only" incremental — that silently dropped
postponed / late-finalised matches whose played date isn't the exact
``execution_date``. Re-runs are idempotent (overwrite, never append); Bronze +
Silver upsert downstream.
"""

import io
import json
import logging
from dataclasses import dataclass

import requests
from minio import Minio
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.config.settings import get_settings
from src.utils.minio_client import create_minio_client, ensure_bucket_exists

logger = logging.getLogger(__name__)

STAGING_BUCKET = "staging"
BASE_PREFIX = "brasileirao"

# GE internal API host. The per-edition campeonato UUID + fase slug come from
# the season registry (see `load_seasons`); only the host is constant here.
GE_API_HOST = "https://api.globoesporte.globo.com"
TOTAL_ROUNDS = 38


@dataclass(frozen=True)
class Season:
    """One Brasileirão edition: its calendar year + GE source identifiers."""

    year: int
    campeonato_id: str
    fase_slug: str


def _parse_season_filter(raw: str) -> set[int] | None:
    """Parse the ``GE_SEASONS`` CSV filter.

    ``"2025,2026"`` -> ``{2025, 2026}``; empty/blank -> ``None`` (every season).
    """
    raw = (raw or "").strip()
    if not raw:
        return None
    return {int(part) for part in raw.split(",") if part.strip()} or None


def load_seasons(execution_date_str: str) -> list[Season]:
    """Resolve which Brasileirão edition(s) to ingest for this run.

    By default the active edition is **derived from the run's year**: the GE
    championship UUID is stable across editions and only the phase slug changes
    per year, so ``year(execution_date)`` uniquely identifies the live edition.
    This makes the pipeline roll over to the next season automatically — no
    per-year config edit. ``GE_SEASONS`` (CSV of years) overrides this to
    pin/force specific edition(s).

    Args:
        execution_date_str: Date string (YYYY-MM-DD) from Airflow's ``ds``.

    Returns:
        Seasons sorted by year (ascending).
    """
    settings = get_settings()
    override = _parse_season_filter(settings.ge_seasons)
    years = override if override is not None else {int(execution_date_str[:4])}

    seasons = [
        Season(
            year=year,
            campeonato_id=settings.ge_campeonato_id,
            fase_slug=settings.ge_fase_slug_template.format(year=year),
        )
        for year in sorted(years)
    ]
    logger.info("Seasons to ingest: %s (override=%s)", [s.year for s in seasons], settings.ge_seasons or None)
    return seasons


def build_round_url(rodada: int, season: Season) -> str:
    """Build the GE round-fixtures URL for ``season``.

    Args:
        rodada: Round number (1-38).
        season: The edition being fetched (carries its campeonato UUID + slug).

    Returns:
        Fully qualified URL for the round's fixtures JSON.
    """
    return f"{GE_API_HOST}/tabela/{season.campeonato_id}/fase/{season.fase_slug}/rodada/{rodada}/jogos/"


REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}

# Retry policy for the GE internal API: 3 attempts with exponential backoff
# (1s, 2s, 4s) on transient errors, applied per-request. A round that still
# fails after retries is collected and raised at the end of the scan (see
# _fetch_all_matches) — we refuse to silently ingest a partial season (B-03).
# Airflow still owns task-level retries on top of this.
_RETRY_POLICY = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
    raise_on_status=False,
)


def _build_session() -> requests.Session:
    """Build a requests.Session with the standard retry policy mounted."""
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=_RETRY_POLICY)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def _fetch_all_matches(season: Season, session: requests.Session) -> list[dict]:
    """Fetch matches from all 38 rounds of ``season`` via the GE internal API.

    Uses a retry-mounted session so a transient blip on one round is recovered
    in place. If a round still fails after retries, it is recorded and a
    ``RuntimeError`` is raised at the end of the scan: a partial fetch must fail
    the task loudly rather than write an incomplete season that would still slip
    past the Bronze ``row_count >= 1`` gate (B-03).

    Returns a flat list of normalized match dicts. Returns an **empty list** when
    the edition is not available yet (round 1 errors after retries) — the GE
    endpoint 500s for a slug that doesn't resolve, which is the expected
    off-season / not-yet-published state, not a failure to escalate.

    Raises:
        RuntimeError: If the edition exists (round 1 succeeded) but a later round
            fails after retries — a genuine partial fetch we refuse to ingest.
    """
    all_matches: list[dict] = []
    failed_rounds: list[int] = []

    for rodada in range(1, TOTAL_ROUNDS + 1):
        url = build_round_url(rodada, season)
        try:
            resp = session.get(url, headers=REQUEST_HEADERS, timeout=15)
            resp.raise_for_status()
            jogos = resp.json()
        except requests.RequestException as e:
            logger.warning("Season %d: failed to fetch round %d after retries: %s", season.year, rodada, e)
            if rodada == 1:
                # Round 1 doesn't resolve → the edition isn't available (off-season
                # gap before the new season is published, or a past season the GE
                # no longer serves). Short-circuit to "no active edition" without
                # hammering the remaining 37 doomed rounds, and never hard-fail an
                # expected calendar gap.
                logger.warning(
                    "Season %d: round 1 unavailable — treating as no active edition (off-season?).",
                    season.year,
                )
                return []
            failed_rounds.append(rodada)
            continue

        for jogo in jogos:
            match = _normalize_match(jogo, rodada)
            if match:
                all_matches.append(match)

        logger.debug("Season %d round %d: fetched %d matches", season.year, rodada, len(jogos))

    if failed_rounds:
        msg = (
            f"GE API fetch incomplete for season {season.year}: {len(failed_rounds)} round(s) failed after retries "
            f"({failed_rounds}) while the edition is live. Aborting to avoid silently ingesting a partial season."
        )
        logger.error(msg)
        raise RuntimeError(msg)

    logger.info("Season %d: total matches fetched from GE API: %d", season.year, len(all_matches))
    return all_matches


def _normalize_match(jogo: dict, rodada: int) -> dict | None:
    """Normalize a single match JSON from the GE API into our standard schema.

    Preserves home/away order exactly as returned by the source (mandante first).
    """
    try:
        data_str = jogo.get("data_realizacao", "")
        if not data_str:
            return None

        # data_realizacao is like "2026-01-28T19:00"
        match_date = data_str[:10]  # "2026-01-28"

        equipes = jogo.get("equipes", {})
        mandante = equipes.get("mandante", {})
        visitante = equipes.get("visitante", {})

        sede = jogo.get("sede") or {}
        transmissao = jogo.get("transmissao") or {}
        broadcast_info = transmissao.get("broadcast") or {}

        # Determine broadcast channel from label/url
        broadcast_label = broadcast_info.get("label", "")
        broadcast_url = transmissao.get("url", "")

        return {
            "matchweek": rodada,
            "home_team": mandante.get("nome_popular", "Unknown"),
            "home_team_code": mandante.get("sigla", ""),
            "away_team": visitante.get("nome_popular", "Unknown"),
            "away_team_code": visitante.get("sigla", ""),
            "score_home": jogo.get("placar_oficial_mandante"),
            "score_away": jogo.get("placar_oficial_visitante"),
            "date": match_date,
            "kickoff_time": jogo.get("hora_realizacao", ""),
            "stadium": sede.get("nome_popular", "Unknown"),
            "broadcast": broadcast_label,
            "match_url": broadcast_url,
            "match_started": jogo.get("jogo_ja_comecou", False),
            "source": "ge.globo.com",
            "ge_match_id": jogo.get("id"),
        }
    except (KeyError, TypeError) as e:
        logger.warning("Failed to normalize match: %s", e)
        return None


def fetch_and_upload(execution_date_str: str) -> int:
    """Fetch Brasileirão matches and (re)stage every finished match.

    **Full reconcile across every selected season.** Every run uploads *all*
    finished matches up to ``execution_date`` (across all editions in the
    registry / ``GE_SEASONS`` filter), grouped by match date and overwriting
    each date's file idempotently. This deliberately drops the old "today-only"
    incremental: a match not played exactly on an ``execution_date`` — a missed
    daily run, a **postponed** match played weeks after its round, a
    late-finalised score — would otherwise be silently never staged (that's how
    round 18 went missing). Re-uploading is cheap (~one small JSON per
    match-date) and Bronze/Silver upsert idempotently, so the lakehouse always
    reconciles to GE's current finished-match set.

    Args:
        execution_date_str: Date string (YYYY-MM-DD) from Airflow's ``ds``.
            Matches dated after it are treated as "future" relative to this
            logical run and skipped.

    Returns:
        Total number of records uploaded.
    """
    client = create_minio_client()
    ensure_bucket_exists(client, STAGING_BUCKET)

    logger.info("Starting Brasileirão ingestion (full reconcile) for date <= %s", execution_date_str)

    all_matches: list[dict] = []
    with _build_session() as session:
        for season in load_seasons(execution_date_str):
            all_matches.extend(_fetch_all_matches(season, session))

    if not all_matches:
        logger.warning("No matches returned from GE API. Aborting upload.")
        return 0

    # A finished match has an official score and has kicked off.
    finished_matches = [m for m in all_matches if m.get("score_home") is not None and m.get("match_started") is True]
    logger.info("Finished matches available: %d out of %d total", len(finished_matches), len(all_matches))

    # Group every finished match up to execution_date by its match date.
    grouped: dict[str, list[dict]] = {}
    for m in finished_matches:
        match_date = m.get("date", "")
        if match_date and match_date <= execution_date_str:
            grouped.setdefault(match_date, []).append(m)

    logger.info("Dates to (re)stage: %d (up to %s)", len(grouped), execution_date_str)

    total_uploaded = 0
    for match_date in sorted(grouped.keys()):
        day_matches = grouped[match_date]
        object_name = f"{BASE_PREFIX}/{match_date}/matches.json"
        _upload_json(client, STAGING_BUCKET, object_name, day_matches)
        total_uploaded += len(day_matches)
        logger.info("Uploaded %d records to %s", len(day_matches), object_name)

    return total_uploaded


def _upload_json(
    client: Minio,
    bucket: str,
    object_name: str,
    data: list[dict],
) -> None:
    """Upload JSON data to MinIO (idempotent overwrite)."""
    json_bytes = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
    file_obj = io.BytesIO(json_bytes)

    client.put_object(
        bucket_name=bucket,
        object_name=object_name,
        data=file_obj,
        length=len(json_bytes),
        content_type="application/json",
    )
