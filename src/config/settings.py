"""Centralized application settings using pydantic-settings.

All configuration is loaded from environment variables or a `.env` file.
This module provides a single source of truth for all service endpoints,
credentials, and tunable parameters across the application.
"""

from functools import lru_cache

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Passwords that show up in the wild often enough to refuse outright. Adding
# a value here is a hard error, not a warning — keep the list short and only
# include strings that no production environment should ever pick.
_BANNED_PASSWORDS = frozenset(
    {
        "",
        "password",
        "admin",
        "airflow",
        "minio",
        "minio123",
        "changeme",
        "change-me",
        "<change-me>",
        "test",
    }
)

# Minimum length for any credential. 12 chars matches NIST SP 800-63B guidance
# for human-chosen passwords; for randomly generated values 24+ is typical.
_MIN_PASSWORD_LENGTH = 12

# Substrings that mark an unfilled `.env.example` placeholder (e.g.
# `<change-me-min-24-chars>`). These pass the length check but must never ship,
# so they are rejected outright — fail-closed on copy-paste-and-forget.
_PLACEHOLDER_MARKERS = ("change-me", "changeme")


def is_placeholder(value: str) -> bool:
    """Return True if ``value`` is still an unfilled ``.env.example`` placeholder."""
    lowered = value.lower()
    return any(marker in lowered for marker in _PLACEHOLDER_MARKERS)


def _validate_password_strength(value: str, field_name: str) -> str:
    """Reject obviously-weak values for credential-bearing settings.

    Args:
        value: Raw setting value.
        field_name: Field name (used in the error message for diagnostics).

    Returns:
        The original value if it passes the checks.

    Raises:
        ValueError: If the value is in the banned list, an unfilled
            placeholder, or shorter than ``_MIN_PASSWORD_LENGTH`` characters.
    """
    stripped = value.strip()
    if stripped.lower() in _BANNED_PASSWORDS:
        msg = f"{field_name} is set to a banned/default value ({stripped!r}); use a strong, unique password."
        raise ValueError(msg)
    if is_placeholder(stripped):
        msg = f"{field_name} is still the .env.example placeholder; set a real, unique password."
        raise ValueError(msg)
    if len(stripped) < _MIN_PASSWORD_LENGTH:
        msg = f"{field_name} is too short ({len(stripped)} chars); use at least {_MIN_PASSWORD_LENGTH} characters."
        raise ValueError(msg)
    return value


class Settings(BaseSettings):
    """Application settings loaded from environment variables.

    Attributes:
        minio_endpoint: MinIO server address (host:port).
        minio_root_user: MinIO access key.
        minio_root_password: MinIO secret key.
        nessie_uri: Nessie Iceberg REST catalog endpoint.
        spark_master: Spark master URL for job submission.
        spark_driver_memory: Memory allocated to the Spark driver.
        spark_executor_memory: Memory allocated to each Spark executor.
        ge_campeonato_id: Stable GE championship UUID (same across editions).
        ge_fase_slug_template: Per-year phase slug template ({year} placeholder).
        ge_seasons: Optional CSV override of seasons (empty = derive current).
        openlineage_url: OpenLineage collector HTTP endpoint (empty disables).
        openlineage_namespace: OpenLineage namespace tag for emitted events.
        alert_webhook_url: Slack incoming webhook for operational alerts (empty
            disables — alerts fall back to log-only).
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # MinIO
    minio_endpoint: str = "minio:9000"
    minio_root_user: str
    minio_root_password: str
    # TLS toggle for the MinIO client. Default False for local docker-compose
    # (HTTP). Set MINIO_SECURE=true in staging/prod environments where MinIO
    # (or the upstream S3 service) is fronted by HTTPS.
    minio_secure: bool = False
    # Data-plane service account (F2-1, least privilege). When set, Spark /
    # Dremio / the app authenticate to MinIO with these scoped credentials
    # (RW on the staging + warehouse buckets only) instead of root, so a
    # compromised job cannot administer the object store. Empty (default) =
    # fall back to root — see the `s3_access_key` / `s3_secret_key` properties.
    minio_access_key: str = ""
    minio_secret_key: str = ""

    # Nessie
    nessie_uri: str = "http://nessie:19120/api/v2"

    # Spark
    spark_master: str = "spark://spark-master:7077"
    spark_driver_memory: str = "2g"
    spark_executor_memory: str = "2g"

    # Brasileirão (GE / Globo Esporte) source — current-season auto-derivation.
    # The Brasileirão championship UUID is STABLE across editions; only the
    # phase slug changes per year, deterministically. So the active edition is
    # derived from the run's year (see fetch_brasileirao.load_seasons) — no
    # per-season config to maintain, and the pipeline rolls over to the next
    # year automatically. `ge_seasons` is an optional override (CSV of years,
    # e.g. "2026") to pin/force specific edition(s) instead of the current one.
    ge_campeonato_id: str = "d1a37fa4-e948-43a6-ba53-ab24ab3a45b1"
    ge_fase_slug_template: str = "fase-unica-campeonato-brasileiro-{year}"
    ge_seasons: str = ""

    # OpenLineage (P3.6) — observability of pipeline runs.
    # `openlineage_url` empty (default) means "do not phone home" — the Spark
    # listener is NOT registered at all (silent no-op; the JAR isn't on the
    # client-mode driver classpath). Setting a value (e.g. http://marquez:5000)
    # registers the listener and flips it into transmit mode.
    # `openlineage_namespace` groups lineage events from this project together
    # across multiple pipelines and Spark applications.
    openlineage_url: str = ""
    openlineage_namespace: str = "data_lake"

    # Alerting (F-17) — opt-in operational alerts.
    # Empty (default) = log-only, exactly like `openlineage_url`. Set it to a
    # Slack incoming webhook URL to deliver DAG-failure and quarantine alerts.
    alert_webhook_url: str = ""

    @field_validator("minio_root_password")
    @classmethod
    def _reject_weak_minio_password(cls, v: str) -> str:
        return _validate_password_strength(v, "MINIO_ROOT_PASSWORD")

    @field_validator("minio_secret_key")
    @classmethod
    def _reject_weak_minio_svc_secret(cls, v: str) -> str:
        # Empty = service account not configured (data plane falls back to
        # root); only gate real values for strength.
        return _validate_password_strength(v, "MINIO_SECRET_KEY") if v else v

    @property
    def s3_access_key(self) -> str:
        """Data-plane S3 access key: the scoped service account if set, else root."""
        return self.minio_access_key or self.minio_root_user

    @property
    def s3_secret_key(self) -> str:
        """Data-plane S3 secret key: the scoped service account if set, else root."""
        return self.minio_secret_key or self.minio_root_password


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a cached Settings instance (one per process).

    Returns:
        Settings: Application configuration object.
    """
    # pydantic-settings populates required fields from env vars / .env at runtime
    return Settings()  # type: ignore[call-arg]
