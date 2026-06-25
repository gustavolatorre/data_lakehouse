"""Unit tests for Brasileirão Staging layer — API fetch and MinIO upload."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
import responses

from src.staging.fetch_brasileirao import (
    Season,
    _build_session,
    _fetch_all_matches,
    _normalize_match,
    _parse_season_filter,
    build_round_url,
    fetch_and_upload,
    load_seasons,
)

# A throwaway edition used to build deterministic GE URLs in the fetch tests.
SEASON_2026 = Season(year=2026, campeonato_id="test-uuid", fase_slug="fase-2026")


class TestNormalizeMatch:
    """Tests for the _normalize_match function."""

    def test_valid_match(self):
        """Should parse a valid GE JSON match perfectly."""
        mock_ge_json = {
            "data_realizacao": "2026-05-28T19:00",
            "hora_realizacao": "19:00",
            "equipes": {
                "mandante": {"nome_popular": "Vasco", "sigla": "VAS"},
                "visitante": {"nome_popular": "Palmeiras", "sigla": "PAL"},
            },
            "placar_oficial_mandante": 1,
            "placar_oficial_visitante": 0,
            "sede": {"nome_popular": "São Januário"},
            "jogo_ja_comecou": True,
            "id": 12345,
        }

        result = _normalize_match(mock_ge_json, rodada=10)

        assert result is not None
        assert result["matchweek"] == 10
        assert result["home_team"] == "Vasco"
        assert result["home_team_code"] == "VAS"
        assert result["away_team"] == "Palmeiras"
        assert result["away_team_code"] == "PAL"
        assert result["score_home"] == 1
        assert result["score_away"] == 0
        assert result["date"] == "2026-05-28"
        assert result["kickoff_time"] == "19:00"
        assert result["stadium"] == "São Januário"
        assert result["ge_match_id"] == 12345
        assert result["match_started"] is True

    def test_missing_date(self):
        """Should return None if data_realizacao is missing."""
        mock_ge_json = {"equipes": {"mandante": {}, "visitante": {}}}
        result = _normalize_match(mock_ge_json, rodada=1)
        assert result is None

    def test_no_score_yet(self):
        """Should handle matches that haven't started (None scores)."""
        mock_ge_json = {
            "data_realizacao": "2026-06-01T16:00",
            "hora_realizacao": "16:00",
            "equipes": {
                "mandante": {"nome_popular": "Galo", "sigla": "CAM"},
                "visitante": {"nome_popular": "Bahia", "sigla": "BAH"},
            },
            "placar_oficial_mandante": None,
            "placar_oficial_visitante": None,
            "sede": {"nome_popular": "Arena MRV"},
            "jogo_ja_comecou": False,
            "id": 999,
        }

        result = _normalize_match(mock_ge_json, rodada=12)
        assert result is not None
        assert result["score_home"] is None
        assert result["score_away"] is None


class TestParseSeasonFilter:
    """Tests for the GE_SEASONS CSV filter parser."""

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("", None),
            ("   ", None),
            ("2026", {2026}),
            ("2025,2026", {2025, 2026}),
            ("2025, 2026 ", {2025, 2026}),
        ],
    )
    def test_parse(self, raw, expected):
        assert _parse_season_filter(raw) == expected


class TestBuildRoundUrl:
    """The round URL must carry the season's campeonato UUID + fase slug."""

    def test_url_includes_season_identifiers(self):
        url = build_round_url(7, SEASON_2026)
        assert "test-uuid" in url
        assert "fase-2026" in url
        assert "/rodada/7/" in url


class TestLoadSeasons:
    """Tests for current-season auto-derivation (+ GE_SEASONS override)."""

    @staticmethod
    def _settings(ge_seasons: str = ""):
        return SimpleNamespace(
            ge_campeonato_id="stable-uuid",
            ge_fase_slug_template="fase-unica-campeonato-brasileiro-{year}",
            ge_seasons=ge_seasons,
        )

    @patch("src.staging.fetch_brasileirao.get_settings")
    def test_derives_current_season_from_execution_year(self, mock_settings):
        mock_settings.return_value = self._settings()
        seasons = load_seasons("2027-05-10")
        assert [s.year for s in seasons] == [2027]
        assert seasons[0].campeonato_id == "stable-uuid"
        assert seasons[0].fase_slug == "fase-unica-campeonato-brasileiro-2027"

    @patch("src.staging.fetch_brasileirao.get_settings")
    def test_rolls_over_to_next_year_automatically(self, mock_settings):
        """The 2026→2027 turnover needs no config edit — the year drives the slug."""
        mock_settings.return_value = self._settings()
        assert [s.year for s in load_seasons("2026-08-01")] == [2026]
        assert [s.year for s in load_seasons("2027-08-01")] == [2027]

    @patch("src.staging.fetch_brasileirao.get_settings")
    def test_ge_seasons_override_pins_edition(self, mock_settings):
        """An explicit GE_SEASONS override wins over the execution year."""
        mock_settings.return_value = self._settings(ge_seasons="2026")
        seasons = load_seasons("2027-05-10")
        assert [s.year for s in seasons] == [2026]

    @patch("src.staging.fetch_brasileirao.get_settings")
    def test_ge_seasons_override_multiple(self, mock_settings):
        mock_settings.return_value = self._settings(ge_seasons="2025,2026")
        assert [s.year for s in load_seasons("2027-01-01")] == [2025, 2026]


class TestFetchAllMatches:
    """Tests for the _fetch_all_matches function."""

    @responses.activate
    @patch("src.staging.fetch_brasileirao.TOTAL_ROUNDS", 2)
    def test_fetch_all_rounds_success(self):
        """Should iterate through rounds and accumulate matches."""
        responses.add(
            responses.GET,
            build_round_url(1, SEASON_2026),
            json=[
                {
                    "data_realizacao": "2026-04-10T16:00",
                    "equipes": {"mandante": {"nome_popular": "A"}, "visitante": {"nome_popular": "B"}},
                }
            ],
            status=200,
        )
        responses.add(
            responses.GET,
            build_round_url(2, SEASON_2026),
            json=[
                {
                    "data_realizacao": "2026-04-17T16:00",
                    "equipes": {"mandante": {"nome_popular": "C"}, "visitante": {"nome_popular": "D"}},
                }
            ],
            status=200,
        )

        matches = _fetch_all_matches(SEASON_2026, _build_session())
        assert len(matches) == 2
        assert matches[0]["home_team"] == "A"
        assert matches[1]["home_team"] == "C"

    @responses.activate
    @patch("src.staging.fetch_brasileirao.TOTAL_ROUNDS", 1)
    def test_round1_unavailable_is_benign(self):
        """Off-season / not-yet-published edition: round 1 errors → empty list, no raise.

        The GE endpoint 500s for a slug that doesn't resolve yet — that's an
        expected calendar gap, not a failure that should redden the daily DAG.
        """
        responses.add(responses.GET, build_round_url(1, SEASON_2026), json={"error": "Not found"}, status=404)

        matches = _fetch_all_matches(SEASON_2026, _build_session())
        assert matches == []

    @responses.activate
    @patch("src.staging.fetch_brasileirao.TOTAL_ROUNDS", 2)
    def test_partial_after_round1_raises_no_silent_partial(self):
        """B-03: edition is live (round 1 OK) but a later round fails → RuntimeError.

        A genuine mid-season partial fetch must not silently ingest an
        incomplete season that the Bronze ``row_count >= 1`` gate would pass.
        """
        responses.add(
            responses.GET,
            build_round_url(1, SEASON_2026),
            json=[
                {
                    "data_realizacao": "2026-04-10T16:00",
                    "equipes": {"mandante": {"nome_popular": "A"}, "visitante": {"nome_popular": "B"}},
                }
            ],
            status=200,
        )
        responses.add(responses.GET, build_round_url(2, SEASON_2026), json={"error": "not found"}, status=404)

        with pytest.raises(RuntimeError, match="incomplete"):
            _fetch_all_matches(SEASON_2026, _build_session())


class TestFetchAndUpload:
    """Tests for the main fetch_and_upload orchestration (full reconcile)."""

    @patch("src.staging.fetch_brasileirao.load_seasons", return_value=[SEASON_2026])
    @patch("src.staging.fetch_brasileirao.create_minio_client")
    @patch("src.staging.fetch_brasileirao.ensure_bucket_exists")
    @patch("src.staging.fetch_brasileirao._fetch_all_matches")
    def test_uploads_all_finished_up_to_execution_date(self, mock_fetch, mock_ensure, mock_client, mock_seasons):
        """Full reconcile: every finished match dated <= execution_date is
        (re)staged, grouped by date; future-dated matches are excluded."""
        mock_minio = MagicMock()
        mock_client.return_value = mock_minio

        # Matches from 2 dates, plus one future date (2026-05-30).
        mock_fetch.return_value = [
            {"date": "2026-05-20", "home_team": "A", "score_home": 1, "match_started": True},
            {"date": "2026-05-20", "home_team": "B", "score_home": 1, "match_started": True},
            {"date": "2026-05-21", "home_team": "C", "score_home": 1, "match_started": True},
            {"date": "2026-05-30", "home_team": "D", "score_home": 1, "match_started": True},  # Future
        ]

        total = fetch_and_upload("2026-05-29")

        assert total == 3  # Excludes the future match
        assert mock_minio.put_object.call_count == 2  # 1 for 05-20, 1 for 05-21

    @patch("src.staging.fetch_brasileirao.load_seasons", return_value=[SEASON_2026])
    @patch("src.staging.fetch_brasileirao.create_minio_client")
    @patch("src.staging.fetch_brasileirao.ensure_bucket_exists")
    @patch("src.staging.fetch_brasileirao._fetch_all_matches")
    def test_restages_old_dates_every_run(self, mock_fetch, mock_ensure, mock_client, mock_seasons):
        """The bug fix: there is NO 'today-only' filter. A late-finalised /
        postponed match whose played date is far in the past is still re-staged
        every run, so it can never be silently lost.
        """
        mock_minio = MagicMock()
        mock_client.return_value = mock_minio

        mock_fetch.return_value = [
            {"date": "2026-04-01", "home_team": "Old", "score_home": 2, "match_started": True},
            {"date": "2026-05-28", "home_team": "New", "score_home": 1, "match_started": True},
        ]

        total = fetch_and_upload("2026-05-28")

        assert total == 2
        # Both dates get their own file — the old date is NOT skipped.
        uploaded = {c.kwargs["object_name"] for c in mock_minio.put_object.call_args_list}
        assert uploaded == {
            "brasileirao/2026-04-01/matches.json",
            "brasileirao/2026-05-28/matches.json",
        }

    @patch("src.staging.fetch_brasileirao.load_seasons", return_value=[SEASON_2026])
    @patch("src.staging.fetch_brasileirao.create_minio_client")
    @patch("src.staging.fetch_brasileirao.ensure_bucket_exists")
    @patch("src.staging.fetch_brasileirao._fetch_all_matches")
    def test_skips_unfinished_matches(self, mock_fetch, mock_ensure, mock_client, mock_seasons):
        """Only matches with a score AND match_started=True are staged."""
        mock_minio = MagicMock()
        mock_client.return_value = mock_minio

        mock_fetch.return_value = [
            {"date": "2026-05-28", "home_team": "Played", "score_home": 1, "match_started": True},
            {"date": "2026-05-28", "home_team": "NotStarted", "score_home": None, "match_started": False},
        ]

        total = fetch_and_upload("2026-05-28")

        assert total == 1
        assert mock_minio.put_object.call_count == 1

    @patch("src.staging.fetch_brasileirao.load_seasons", return_value=[SEASON_2026])
    @patch("src.staging.fetch_brasileirao.create_minio_client")
    @patch("src.staging.fetch_brasileirao.ensure_bucket_exists")
    @patch("src.staging.fetch_brasileirao._fetch_all_matches")
    def test_returns_zero_when_no_matches(self, mock_fetch, mock_ensure, mock_client, mock_seasons):
        """No matches from the API → nothing uploaded, total 0."""
        mock_minio = MagicMock()
        mock_client.return_value = mock_minio
        mock_fetch.return_value = []

        total = fetch_and_upload("2026-05-28")

        assert total == 0
        assert mock_minio.put_object.call_count == 0
