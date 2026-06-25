"""Unit tests for ``src.utils.nessie_branch``.

The real Nessie server is mocked via ``responses`` — we don't want unit
tests to need a live container, and the value here is asserting the
**call shape** (right URL, right body, right headers) rather than
network round-trips.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
import responses

from src.utils.nessie_branch import (
    NessieAPIError,
    branch_exists,
    build_branch_name,
    create_branch,
    drop_branch,
    merge_branch,
)


@pytest.fixture
def fake_settings():
    """Pin the Nessie URI so the URLs we assert match exactly."""
    with patch("src.utils.nessie_branch.get_settings") as g:
        g.return_value.nessie_uri = "http://nessie:19120/api/v2"
        yield g.return_value


# ---------------------------------------------------------------------------
# build_branch_name
# ---------------------------------------------------------------------------


class TestBuildBranchName:
    def test_normalises_date(self):
        name = build_branch_name(dag_id="bronze_silver", execution_date="2026-04-29")
        assert name == "etl_bronze_silver_2026_04_29"

    def test_passes_validation(self):
        # Validation re-runs inside the API calls, so any name we build
        # here must also pass `_validate_branch_name`.
        assert build_branch_name("xyz", "2099-01-02") == "etl_xyz_2099_01_02"


# ---------------------------------------------------------------------------
# Name validation
# ---------------------------------------------------------------------------


class TestNameValidation:
    @pytest.mark.parametrize("bad", ["foo/bar", "with space", "tab\there", "back\\slash"])
    def test_rejects_url_special_characters(self, bad, fake_settings):
        with pytest.raises(ValueError, match="Invalid Nessie branch name"):
            create_branch(bad)


# ---------------------------------------------------------------------------
# create_branch
# ---------------------------------------------------------------------------


def _mock_main_history_nonempty() -> None:
    """Pretend ``main`` already has at least one commit so the P3.12 bootstrap path is skipped."""
    responses.add(
        responses.GET,
        "http://nessie:19120/api/v2/trees/main/history",
        json={"logEntries": [{"commitMeta": {"hash": "real_commit", "message": "init"}}]},
        status=200,
    )


class TestCreateBranch:
    @responses.activate
    def test_creates_when_missing(self, fake_settings):
        # branch_exists check returns 404 -> doesn't exist
        responses.add(
            responses.GET,
            "http://nessie:19120/api/v2/trees/etl_x_2026_04_29",
            json={"reference": {"type": "BRANCH", "hash": "abc"}},
            status=404,
        )
        # P3.12: source already has commit history -> bootstrap path skipped.
        _mock_main_history_nonempty()
        # GET source to resolve hash
        responses.add(
            responses.GET,
            "http://nessie:19120/api/v2/trees/main",
            json={"reference": {"type": "BRANCH", "hash": "mainHASH"}},
            status=200,
        )
        # POST creates the branch
        responses.add(
            responses.POST,
            "http://nessie:19120/api/v2/trees",
            json={"reference": {"type": "BRANCH", "name": "etl_x_2026_04_29", "hash": "abc"}},
            status=200,
        )

        create_branch("etl_x_2026_04_29", source_ref="main")

        post_call = next(c for c in responses.calls if c.request.method == "POST")
        # Nessie v2: new branch name + type go in the query string.
        assert "name=etl_x_2026_04_29" in post_call.request.url
        assert "type=BRANCH" in post_call.request.url
        # Body carries the source Reference (its own type + hash).
        body = post_call.request.body.decode()
        assert "BRANCH" in body
        assert "mainHASH" in body
        assert "main" in body  # source ref name in the Reference body

    @responses.activate
    def test_is_noop_when_branch_exists(self, fake_settings, caplog):
        # branch_exists -> 200 + BRANCH
        responses.add(
            responses.GET,
            "http://nessie:19120/api/v2/trees/etl_existing",
            json={"reference": {"type": "BRANCH", "hash": "abc"}},
            status=200,
        )

        create_branch("etl_existing", source_ref="main")

        # No POST was issued — and crucially the bootstrap path didn't run
        # either, because we short-circuit before checking source ref state.
        assert all(c.request.method != "POST" for c in responses.calls)

    @responses.activate
    def test_raises_on_5xx(self, fake_settings):
        # branch_exists 404 -> proceed
        responses.add(
            responses.GET,
            "http://nessie:19120/api/v2/trees/etl_x",
            json={},
            status=404,
        )
        _mock_main_history_nonempty()
        # GET source hash 200
        responses.add(
            responses.GET,
            "http://nessie:19120/api/v2/trees/main",
            json={"reference": {"type": "BRANCH", "hash": "h"}},
            status=200,
        )
        # POST returns 500
        responses.add(
            responses.POST,
            "http://nessie:19120/api/v2/trees",
            json={"error": "boom"},
            status=500,
        )

        with pytest.raises(NessieAPIError, match="status=500"):
            create_branch("etl_x", source_ref="main")

    @responses.activate
    def test_bootstraps_main_when_history_is_empty(self, fake_settings):
        """P3.12 — if ``main`` has zero commits, plant a bootstrap commit before branching.

        Otherwise the subsequent merge_branch back into main fails with
        REFERENCE_NOT_FOUND ("no common ancestor"). This test pins the
        contract: a new branch creation against an empty ref must trigger
        one bootstrap POST on main's history, then proceed normally.
        """
        # branch_exists -> 404 (etl branch doesn't exist yet)
        responses.add(
            responses.GET,
            "http://nessie:19120/api/v2/trees/etl_x_2026_04_29",
            json={},
            status=404,
        )
        # main history is EMPTY -> bootstrap path triggers
        responses.add(
            responses.GET,
            "http://nessie:19120/api/v2/trees/main/history",
            json={"logEntries": []},
            status=200,
        )
        # _ensure_ref_has_history fetches main head to obtain the expected-hash
        responses.add(
            responses.GET,
            "http://nessie:19120/api/v2/trees/main",
            json={"reference": {"type": "BRANCH", "hash": "EMPTY_HEAD"}},
            status=200,
        )
        # bootstrap commit on main
        responses.add(
            responses.POST,
            "http://nessie:19120/api/v2/trees/main@EMPTY_HEAD/history/commit",
            json={"targetBranch": {"type": "BRANCH", "name": "main", "hash": "POST_BOOTSTRAP_HEAD"}},
            status=200,
        )
        # _ref_hash for the source ref AFTER bootstrap (fresh main head)
        responses.add(
            responses.GET,
            "http://nessie:19120/api/v2/trees/main",
            json={"reference": {"type": "BRANCH", "hash": "POST_BOOTSTRAP_HEAD"}},
            status=200,
        )
        # create_branch POST itself
        responses.add(
            responses.POST,
            "http://nessie:19120/api/v2/trees",
            json={"reference": {"type": "BRANCH", "name": "etl_x_2026_04_29", "hash": "etl_h"}},
            status=200,
        )

        create_branch("etl_x_2026_04_29", source_ref="main")

        # We expect exactly two POSTs in order: bootstrap commit on main, then branch creation.
        posts = [c for c in responses.calls if c.request.method == "POST"]
        assert len(posts) == 2
        assert "main@EMPTY_HEAD/history/commit" in posts[0].request.url
        assert "/trees?name=etl_x_2026_04_29" in posts[1].request.url
        # Bootstrap body carries the placeholder namespace and a recognisable author.
        bootstrap_body = posts[0].request.body.decode()
        assert "bootstrap" in bootstrap_body
        assert "NAMESPACE" in bootstrap_body
        assert "data-lake-init" in bootstrap_body


# ---------------------------------------------------------------------------
# drop_branch
# ---------------------------------------------------------------------------


class TestDropBranch:
    @responses.activate
    def test_drops_when_exists(self, fake_settings):
        # branch_exists -> exists
        responses.add(
            responses.GET,
            "http://nessie:19120/api/v2/trees/etl_x",
            json={"reference": {"type": "BRANCH", "hash": "h1"}},
            status=200,
        )
        # DELETE — 200 with a JSON body instead of 204 No-Content; the
        # `responses` library can't replay an empty 204 cleanly so we mock
        # what Nessie does in practice (returns the deleted ref).
        responses.add(
            responses.DELETE,
            "http://nessie:19120/api/v2/trees/etl_x@h1",
            json={"deleted": True},
            status=200,
        )

        drop_branch("etl_x")

        delete_call = next(c for c in responses.calls if c.request.method == "DELETE")
        assert "etl_x@h1" in delete_call.request.url

    @responses.activate
    def test_noop_when_missing(self, fake_settings):
        responses.add(
            responses.GET,
            "http://nessie:19120/api/v2/trees/etl_missing",
            json={},
            status=404,
        )

        drop_branch("etl_missing")

        assert all(c.request.method != "DELETE" for c in responses.calls)


# ---------------------------------------------------------------------------
# merge_branch
# ---------------------------------------------------------------------------


class TestMergeBranch:
    @responses.activate
    def test_merges_into_main(self, fake_settings):
        # GET source hash
        responses.add(
            responses.GET,
            "http://nessie:19120/api/v2/trees/etl_x",
            json={"reference": {"type": "BRANCH", "hash": "src"}},
            status=200,
        )
        # GET target hash
        responses.add(
            responses.GET,
            "http://nessie:19120/api/v2/trees/main",
            json={"reference": {"type": "BRANCH", "hash": "tgt"}},
            status=200,
        )
        responses.add(
            responses.POST,
            "http://nessie:19120/api/v2/trees/main@tgt/history/merge",
            json={"merged": True},
            status=200,
        )

        merge_branch("etl_x", target="main")

        post = next(c for c in responses.calls if c.request.method == "POST")
        assert "main@tgt/history/merge" in post.request.url
        body = post.request.body.decode()
        assert "fromRefName" in body
        assert "etl_x" in body
        assert "src" in body  # fromHash
        # P3.14: FORCE mode lets the ETL pattern (isolated branch is source-of-
        # truth) keep working across consecutive runs. NORMAL bails on
        # historical key overlap even when target hasn't moved since last merge.
        assert "FORCE" in body

    @responses.activate
    def test_raises_on_conflict(self, fake_settings):
        responses.add(
            responses.GET,
            "http://nessie:19120/api/v2/trees/etl_x",
            json={"reference": {"type": "BRANCH", "hash": "src"}},
            status=200,
        )
        responses.add(
            responses.GET,
            "http://nessie:19120/api/v2/trees/main",
            json={"reference": {"type": "BRANCH", "hash": "tgt"}},
            status=200,
        )
        responses.add(
            responses.POST,
            "http://nessie:19120/api/v2/trees/main@tgt/history/merge",
            json={"reason": "conflict"},
            status=409,
        )

        with pytest.raises(NessieAPIError, match="status=409"):
            merge_branch("etl_x", target="main")


# ---------------------------------------------------------------------------
# branch_exists
# ---------------------------------------------------------------------------


class TestBranchExists:
    @responses.activate
    def test_true_for_branch(self, fake_settings):
        responses.add(
            responses.GET,
            "http://nessie:19120/api/v2/trees/etl_x",
            json={"reference": {"type": "BRANCH", "hash": "h"}},
            status=200,
        )
        assert branch_exists("etl_x") is True

    @responses.activate
    def test_false_for_404(self, fake_settings):
        responses.add(
            responses.GET,
            "http://nessie:19120/api/v2/trees/etl_x",
            json={},
            status=404,
        )
        assert branch_exists("etl_x") is False

    @responses.activate
    def test_false_for_tag(self, fake_settings):
        """A TAG ref is not a BRANCH — function should distinguish."""
        responses.add(
            responses.GET,
            "http://nessie:19120/api/v2/trees/etl_x",
            json={"reference": {"type": "TAG", "hash": "h"}},
            status=200,
        )
        assert branch_exists("etl_x") is False
