"""Pre-flight credential strength check for the local stack.

``make validate-secrets`` runs this before ``make up`` to catch weak, default,
or placeholder passwords in ``.env`` *before* they reach MinIO, Postgres,
Dremio, or the Airflow SimpleAuthManager. It reuses the same banned-list +
min-length rule that ``src/config/settings.py`` enforces on the MinIO password,
extending the coverage to every user-chosen credential.

The Fernet / JWT / webserver-secret keys in ``airflow.env`` are out of scope:
``make init-secrets`` generates them and they are strong by construction.

Exit code 0 = all good; 1 = at least one credential is missing or weak.
"""

from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO_ROOT))

from dotenv import dotenv_values  # noqa: E402  (after sys.path bootstrap)

from src.config.settings import _validate_password_strength, is_placeholder  # noqa: E402

# Every user-chosen secret read from .env. Keep in sync with .env.example.
_CREDENTIALS = (
    "MINIO_ROOT_PASSWORD",
    "POSTGRES_PASSWORD",
    "DREMIO_ADMIN_PASSWORD",
    "AIRFLOW_PASSWORD",
)


def main() -> int:
    env_path = _REPO_ROOT / ".env"
    if not env_path.exists():
        sys.stderr.write(f"ERROR: {env_path} not found — copy .env.example to .env first.\n")
        return 1

    values = dict(dotenv_values(env_path))
    failures: list[str] = []

    for name in _CREDENTIALS:
        value = values.get(name)
        if not value:
            failures.append(f"{name}: missing or empty")
            continue
        # Catch the .env.example placeholder (`<change-me-min-24-chars>`), which
        # is long enough to pass the length rule but must never ship.
        if is_placeholder(value):
            failures.append(f"{name}: still set to the .env.example placeholder")
            continue
        try:
            _validate_password_strength(value, name)
        except ValueError as exc:
            failures.append(f"{name}: {exc}")
        else:
            print(f"OK   {name}")

    if failures:
        sys.stderr.write("\nWeak or missing credentials:\n")
        for failure in failures:
            sys.stderr.write(f"  FAIL {failure}\n")
        sys.stderr.write("\nFix them in .env and re-run `make validate-secrets`.\n")
        return 1

    print("\nAll credentials passed the strength check.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
