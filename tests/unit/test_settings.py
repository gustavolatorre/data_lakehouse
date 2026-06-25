"""Unit tests for ``src.config.settings``.

These tests isolate Settings from the project ``.env`` by chdir'ing to an
empty tmp directory before instantiation. ``env_file=".env"`` is therefore a
no-op and only the test's monkeypatched environment variables are read.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from src.config import settings as settings_module


@pytest.fixture(autouse=True)
def _clear_settings_cache():
    """Reset the lru_cache between tests so env tweaks land."""
    settings_module.get_settings.cache_clear()
    yield
    settings_module.get_settings.cache_clear()


@pytest.fixture
def clean_env(monkeypatch, tmp_path):
    """chdir to an empty dir + set the bare-minimum required env vars."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("MINIO_ROOT_USER", "test-user")
    monkeypatch.setenv("MINIO_ROOT_PASSWORD", "test-pass-strong-enough")
    # Clean any optional vars that other tests might have leaked
    for var in (
        "MINIO_SECURE",
        "MINIO_ENDPOINT",
        "NESSIE_URI",
        "SPARK_MASTER",
        "SPARK_DRIVER_MEMORY",
        "SPARK_EXECUTOR_MEMORY",
        "GE_SEASONS",
        "GE_CAMPEONATO_ID",
        "GE_FASE_SLUG_TEMPLATE",
        "MINIO_ACCESS_KEY",
        "MINIO_SECRET_KEY",
    ):
        monkeypatch.delenv(var, raising=False)


class TestSettingsLoading:
    def test_loads_required_env_vars(self, clean_env):
        s = settings_module.Settings()  # type: ignore[call-arg]
        assert s.minio_root_user == "test-user"
        assert s.minio_root_password == "test-pass-strong-enough"  # pragma: allowlist secret

    def test_fails_when_required_missing(self, monkeypatch, tmp_path):
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("MINIO_ROOT_USER", raising=False)
        monkeypatch.delenv("MINIO_ROOT_PASSWORD", raising=False)
        with pytest.raises(ValidationError):
            settings_module.Settings()  # type: ignore[call-arg]

    def test_default_minio_secure_is_false(self, clean_env):
        s = settings_module.Settings()  # type: ignore[call-arg]
        assert s.minio_secure is False

    def test_minio_secure_can_be_overridden(self, clean_env, monkeypatch):
        monkeypatch.setenv("MINIO_SECURE", "true")
        s = settings_module.Settings()  # type: ignore[call-arg]
        assert s.minio_secure is True

    def test_default_nessie_uri(self, clean_env):
        s = settings_module.Settings()  # type: ignore[call-arg]
        assert s.nessie_uri == "http://nessie:19120/api/v2"

    def test_default_ge_source(self, clean_env):
        s = settings_module.Settings()  # type: ignore[call-arg]
        assert s.ge_campeonato_id == "d1a37fa4-e948-43a6-ba53-ab24ab3a45b1"
        assert s.ge_fase_slug_template == "fase-unica-campeonato-brasileiro-{year}"
        assert s.ge_seasons == ""

    def test_default_spark_resources(self, clean_env):
        s = settings_module.Settings()  # type: ignore[call-arg]
        assert s.spark_master == "spark://spark-master:7077"
        assert s.spark_driver_memory == "2g"
        assert s.spark_executor_memory == "2g"


class TestGetSettingsCache:
    def test_returns_cached_instance(self, clean_env):
        first = settings_module.get_settings()
        second = settings_module.get_settings()
        assert first is second, "get_settings must return the same cached object"

    def test_cache_can_be_cleared(self, clean_env):
        first = settings_module.get_settings()
        settings_module.get_settings.cache_clear()
        second = settings_module.get_settings()
        assert first is not second, "after cache_clear, get_settings must return a fresh object"


class TestPasswordStrengthValidator:
    """Settings must refuse banned defaults and too-short MinIO passwords."""

    @pytest.mark.parametrize(
        "weak",
        ["password", "admin", "minio", "minio123", "changeme", "<change-me>"],
    )
    def test_rejects_banned_value(self, clean_env, monkeypatch, weak):
        monkeypatch.setenv("MINIO_ROOT_PASSWORD", weak)
        with pytest.raises(ValidationError, match="banned/default"):
            settings_module.Settings()  # type: ignore[call-arg]

    def test_rejects_short_password(self, clean_env, monkeypatch):
        monkeypatch.setenv("MINIO_ROOT_PASSWORD", "short")
        with pytest.raises(ValidationError, match="too short"):
            settings_module.Settings()  # type: ignore[call-arg]

    def test_accepts_strong_password(self, clean_env, monkeypatch):
        monkeypatch.setenv("MINIO_ROOT_PASSWORD", "K9!fJ8mP2nQ7rT4w")
        s = settings_module.Settings()  # type: ignore[call-arg]
        assert s.minio_root_password == "K9!fJ8mP2nQ7rT4w"  # pragma: allowlist secret

    def test_case_insensitive_ban(self, clean_env, monkeypatch):
        """Banned values are matched case-insensitively (also too short at 8 chars)."""
        monkeypatch.setenv("MINIO_ROOT_PASSWORD", "PASSWORD")
        with pytest.raises(ValidationError):
            settings_module.Settings()  # type: ignore[call-arg]


class TestPlaceholderRejection:
    """The .env.example placeholder must fail closed at the validator (F-13).

    `<change-me-min-24-chars>` is >12 chars and not exact-banned, so without an
    explicit check it would slip past the strength validator. Lock that shut.
    """

    @pytest.mark.parametrize(
        "placeholder",
        ["<change-me-min-24-chars>", "change-me", "CHANGE-ME", "changeme", "x-changeme-y"],
    )
    def test_is_placeholder_detects(self, placeholder):
        assert settings_module.is_placeholder(placeholder) is True

    def test_real_password_is_not_placeholder(self):
        assert settings_module.is_placeholder("K9!fJ8mP2nQ7rT4w") is False

    def test_settings_rejects_placeholder_password(self, clean_env, monkeypatch):
        monkeypatch.setenv("MINIO_ROOT_PASSWORD", "<change-me-min-24-chars>")
        with pytest.raises(ValidationError, match="placeholder"):
            settings_module.Settings()  # type: ignore[call-arg]


class TestMinioServiceAccount:
    """Data-plane creds prefer the scoped service account, falling back to root (F2-1)."""

    def test_s3_creds_fall_back_to_root_when_unset(self, clean_env):
        s = settings_module.Settings()  # type: ignore[call-arg]
        assert s.s3_access_key == "test-user"
        assert s.s3_secret_key == "test-pass-strong-enough"  # pragma: allowlist secret

    def test_s3_creds_use_service_account_when_set(self, clean_env, monkeypatch):
        monkeypatch.setenv("MINIO_ACCESS_KEY", "lakehouse-app")
        monkeypatch.setenv("MINIO_SECRET_KEY", "svc-secret-strong-123456")
        s = settings_module.Settings()  # type: ignore[call-arg]
        assert s.s3_access_key == "lakehouse-app"
        assert s.s3_secret_key == "svc-secret-strong-123456"  # pragma: allowlist secret

    def test_empty_service_secret_is_allowed(self, clean_env):
        # The default (unset) MINIO_SECRET_KEY must not trip the strength gate.
        s = settings_module.Settings()  # type: ignore[call-arg]
        assert s.minio_secret_key == ""

    def test_weak_service_secret_is_rejected(self, clean_env, monkeypatch):
        monkeypatch.setenv("MINIO_SECRET_KEY", "short")
        with pytest.raises(ValidationError, match="too short"):
            settings_module.Settings()  # type: ignore[call-arg]

    def test_placeholder_service_secret_is_rejected(self, clean_env, monkeypatch):
        monkeypatch.setenv("MINIO_SECRET_KEY", "<change-me-min-24-chars>")
        with pytest.raises(ValidationError, match="placeholder"):
            settings_module.Settings()  # type: ignore[call-arg]
