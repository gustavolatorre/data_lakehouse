"""Thin wrapper over the Nessie REST API for branch lifecycle.

Each Bronze/Silver run creates an **isolated branch** off ``main``, writes
to it, and merges back only after the quality gates have passed. If a step
fails the branch is dropped, leaving ``main`` untouched. This is the
canonical "transactional ETL" pattern Nessie was built for; on-disk it
costs nothing (branches are just metadata pointers).

We hit the REST API directly instead of going through nessie-spark-extensions
because that JAR was deliberately removed from the Spark image (its
NessieCatalog is already bundled in the Iceberg runtime).

API reference: https://projectnessie.org/nessie-latest/api/
We use the v2 endpoints (the Nessie 0.79+ default).
"""

from __future__ import annotations

import logging
import re
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.config.settings import get_settings

logger = logging.getLogger(__name__)

# Branch name sanity. Nessie accepts most strings but we keep ours strict
# so they're safe in URLs without %-encoding, and so they line up with the
# convention dbt + Airflow already use for asset URIs.
_BRANCH_NAME_RE = re.compile(r"^[A-Za-z0-9_.\-]+$")

# How long we wait for a single Nessie HTTP call. The API is meant to be
# fast (in-memory + Postgres-backed by default); seconds-long latency means
# something is wrong and we'd rather fail than hang the DAG.
_HTTP_TIMEOUT_SECONDS = 15

# Retry on the kinds of transient errors that show up when the Nessie
# pod is rolling. Same policy as the staging fetcher.
_RETRY_POLICY = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST", "DELETE"],
    raise_on_status=False,
)


class NessieAPIError(RuntimeError):
    """Raised when a Nessie HTTP call returns a non-2xx response."""


def _session() -> requests.Session:
    """A retrying ``requests.Session`` aimed at the configured Nessie URI."""
    s = requests.Session()
    adapter = HTTPAdapter(max_retries=_RETRY_POLICY)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


def _base_url() -> str:
    """The catalog base URL — ``settings.nessie_uri`` already includes ``/api/v2``."""
    return get_settings().nessie_uri.rstrip("/")


def _validate_branch_name(name: str) -> None:
    if not _BRANCH_NAME_RE.fullmatch(name):
        msg = (
            f"Invalid Nessie branch name {name!r}: must match {_BRANCH_NAME_RE.pattern}. "
            "Use letters, digits, dot, dash or underscore — no '/' or other URL-special chars."
        )
        raise ValueError(msg)


def build_branch_name(dag_id: str, execution_date: str) -> str:
    """Construct a deterministic branch name for a DAG run.

    Args:
        dag_id: Airflow DAG id (already a valid identifier).
        execution_date: ``YYYY-MM-DD``.

    Returns:
        ``etl_<dag_id>_<execution_date_with_underscores>`` — safe to use as
        a Nessie branch name and as a Spark catalog ref.
    """
    safe_date = execution_date.replace("-", "_")
    return f"etl_{dag_id}_{safe_date}"


def create_branch(name: str, *, source_ref: str = "main") -> None:
    """Create ``name`` off ``source_ref``.

    Idempotent: if the branch already exists the call is a no-op + logs a
    warning. Useful when an Airflow task retries after a partial failure.

    Raises:
        NessieAPIError: On any other non-2xx response.
        ValueError: On a malformed branch name.
    """
    _validate_branch_name(name)

    if branch_exists(name):
        logger.warning("Nessie branch '%s' already exists — reusing it", name)
        return

    # Make sure source_ref has at least one real commit. A freshly
    # provisioned Nessie has main at the NO_ANCESTOR sentinel hash; merging an
    # etl branch back into that state fails with REFERENCE_NOT_FOUND ("no
    # common ancestor in parents of …") because the sentinel isn't a walkable
    # commit. Planting a placeholder commit here is idempotent and means
    # `make down -v && make up` produces a stack the DAG can drive end-to-end
    # without manual setup.
    _ensure_ref_has_history(source_ref)

    # v2 spec: POST /trees?name=<branch>&type=BRANCH with body = the source
    # Reference (its own type + hash). The first attempt at this passed name
    # and type in the body and got "createReference.type: must not be null"
    # back — they're query params in v2, not body fields.
    src_hash = _ref_hash(source_ref)

    url = f"{_base_url()}/trees"
    query = {"name": name, "type": "BRANCH"}
    # Body shape per Reference schema: { "type": "BRANCH", "name": "<src>", "hash": "<hash>" }
    payload = {"type": "BRANCH", "name": source_ref, "hash": src_hash}
    with _session() as s:
        resp = s.post(url, params=query, json=payload, timeout=_HTTP_TIMEOUT_SECONDS)
    _raise_for_status(resp, f"create branch '{name}' from '{source_ref}'")
    logger.info("Created Nessie branch '%s' off '%s' (hash=%s)", name, source_ref, src_hash[:8])


def drop_branch(name: str) -> None:
    """Delete ``name``. No-op + warn if it does not exist."""
    _validate_branch_name(name)

    if not branch_exists(name):
        logger.warning("Nessie branch '%s' does not exist — nothing to drop", name)
        return

    # v2 spec: DELETE /trees/{ref-key} with expected hash header.
    branch_hash = _ref_hash(name)
    url = f"{_base_url()}/trees/{name}@{branch_hash}"
    with _session() as s:
        resp = s.delete(url, timeout=_HTTP_TIMEOUT_SECONDS)
    _raise_for_status(resp, f"drop branch '{name}'")
    logger.info("Dropped Nessie branch '%s'", name)


def merge_branch(source: str, *, target: str = "main") -> None:
    """Merge ``source`` into ``target``.

    Uses ``defaultKeyMergeMode=FORCE`` because our ETL pattern treats the
    source (an isolated ``etl_*`` branch) as the source-of-truth for the
    keys it touched. ``NORMAL`` is the conservative default and refuses
    the second consecutive merge with ``REFERENCE_CONFLICT`` ("the
    following keys have been changed in conflict: …") — Nessie flags any
    historical overlap on the same key, even when ``target`` hasn't
    moved since the previous merge. ``overwritePartitions`` (Bronze) and
    ``MERGE INTO`` (Silver) already guarantee per-day idempotency, so
    taking the source's version is what we actually want.

    Raises:
        NessieAPIError: On any non-2xx response. Genuine 409 conflicts
            (a true 3-way data conflict that even FORCE can't reconcile)
            still surface as ``NessieAPIError`` — caller decides whether
            to fail the run or rebase.
    """
    _validate_branch_name(source)
    _validate_branch_name(target)

    src_hash = _ref_hash(source)
    target_hash = _ref_hash(target)

    # v2 spec: POST /trees/{target}@{expected-hash}/history/merge
    # The `/history/` segment is mandatory in v2 — without it the server
    # returns 404 with an empty body. (v1 used a flat `/merge` path; the
    # first cut at this used that URL and we hit "No URL specified" 404s
    # every run until we corrected it.)
    url = f"{_base_url()}/trees/{target}@{target_hash}/history/merge"
    payload = {
        "fromRefName": source,
        "fromHash": src_hash,
        "defaultKeyMergeMode": "FORCE",
    }
    with _session() as s:
        resp = s.post(url, json=payload, timeout=_HTTP_TIMEOUT_SECONDS)
    _raise_for_status(resp, f"merge '{source}' into '{target}'")
    logger.info("Merged Nessie branch '%s' into '%s'", source, target)


def branch_exists(name: str) -> bool:
    """``True`` when ``name`` resolves to a BRANCH ref on the server."""
    _validate_branch_name(name)
    url = f"{_base_url()}/trees/{name}"
    with _session() as s:
        resp = s.get(url, timeout=_HTTP_TIMEOUT_SECONDS)
    if resp.status_code == 404:
        return False
    _raise_for_status(resp, f"check branch '{name}'")
    data = resp.json()
    # v2 response wraps the reference under "reference".
    ref = data.get("reference") or data
    return bool(ref.get("type") == "BRANCH")


def _ref_hash(name: str) -> str:
    """Resolve ``name`` to its current commit hash."""
    url = f"{_base_url()}/trees/{name}"
    with _session() as s:
        resp = s.get(url, timeout=_HTTP_TIMEOUT_SECONDS)
    _raise_for_status(resp, f"resolve hash for '{name}'")
    data = resp.json()
    ref = data.get("reference") or data
    h = ref.get("hash")
    if not h:
        msg = f"Nessie response for '{name}' missing 'hash' field: {data!r}"
        raise NessieAPIError(msg)
    return str(h)


def _ensure_ref_has_history(ref_name: str) -> None:
    """Plant a no-op bootstrap commit on ``ref_name`` if its commit log is empty.

    Why this exists: a freshly created Nessie ref points at the ``NO_ANCESTOR``
    sentinel hash. Branching off that state and committing on the new branch is
    fine, but **merging back** fails with ``REFERENCE_NOT_FOUND`` ("no common
    ancestor in parents of …") because the sentinel isn't a walkable commit —
    the merge algorithm walks parents of both refs and finds no overlap.

    We detect "empty" by asking for one history record and seeing none. If so
    we POST a single ``CREATE NAMESPACE`` op so the ref now has a real commit
    that any future etl branch will descend from.

    Idempotent on retry: re-running this once the bootstrap is in place is a
    cheap GET-and-skip.

    Args:
        ref_name: Branch (or tag) to inspect/bootstrap. In practice this is
            always ``main``; we accept the arg so callers can opt in
            explicitly and tests can target other refs.
    """
    history_url = f"{_base_url()}/trees/{ref_name}/history?maxRecords=1"
    with _session() as s:
        resp = s.get(history_url, timeout=_HTTP_TIMEOUT_SECONDS)
    _raise_for_status(resp, f"check history of '{ref_name}'")
    if resp.json().get("logEntries"):
        return  # ref already has at least one commit; nothing to do.

    head_hash = _ref_hash(ref_name)
    commit_url = f"{_base_url()}/trees/{ref_name}@{head_hash}/history/commit"
    # Annotated as dict[str, Any] so requests' typed `json=` arg accepts the
    # nested shape. Without the hint mypy narrows to
    # `dict[str, Collection[Collection[str]]]` and rejects it against JsonType.
    payload: dict[str, Any] = {
        "commitMeta": {
            "message": "bootstrap: plant initial commit so future merges resolve a common ancestor",
            "author": "data-lake-init",
        },
        "operations": [
            {
                "type": "PUT",
                "key": {"elements": ["bootstrap"]},
                "content": {"type": "NAMESPACE", "elements": ["bootstrap"]},
            }
        ],
    }
    with _session() as s:
        resp = s.post(commit_url, json=payload, timeout=_HTTP_TIMEOUT_SECONDS)
    _raise_for_status(resp, f"bootstrap commit on '{ref_name}'")
    logger.info("Planted bootstrap commit on Nessie ref '%s'", ref_name)


def _raise_for_status(resp: requests.Response, what: str) -> None:
    """Translate an HTTP failure into a NessieAPIError with context."""
    if 200 <= resp.status_code < 300:
        return

    body: object
    try:
        body = resp.json()
    except ValueError:
        body = resp.text

    msg = f"Nessie API call failed [{what}]: status={resp.status_code} body={body!r}"
    logger.error(msg)
    raise NessieAPIError(msg)
