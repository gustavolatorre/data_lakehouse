"""Generate /opt/airflow/simple_auth_passwords.json for the SimpleAuthManager.

Airflow 3.x's SimpleAuthManager reads usernames/passwords from a JSON file and
authenticates with a **direct plaintext string comparison** — it does NOT
support hashed passwords (bcrypt or otherwise). Storing a hash here would make
every login fail with 401, so the password is written in plaintext. Exposure is
mitigated by (1) keeping the file at a container-internal path that is never
mounted to the host and (2) mode 0600. Replacing SimpleAuthManager with a real
auth backend is tracked as roadmap item P3.10. Idempotent.

Reads ``AIRFLOW_USER`` and ``AIRFLOW_PASSWORD`` from the environment. Falls back
to ``admin`` / ``airflow`` only if they're unset — but that path is meant for
local smoke tests and should never run in production.
"""

from __future__ import annotations

import json
import os
import stat
import sys
from pathlib import Path

OUTPUT_PATH = Path("/opt/airflow/simple_auth_passwords.json")


def main() -> int:
    user = os.environ.get("AIRFLOW_USER")
    password = os.environ.get("AIRFLOW_PASSWORD")

    # Fail fast instead of silently falling back to insecure defaults: an unset
    # credential almost always means a misconfigured .env, and shipping an
    # "admin" / "airflow" login is worse than refusing to start.
    if not user or not password:
        sys.stderr.write(
            "ERROR: AIRFLOW_USER and AIRFLOW_PASSWORD must both be set "
            "(copy .env.example to .env and fill them in). Refusing to start "
            "with insecure defaults.\n"
        )
        return 1

    if password in ("airflow", "admin", "password", "changeme"):
        # Weak but explicitly chosen — warn loudly, but NEVER echo the value
        # (clear-text logging of a credential; CodeQL py/clear-text-logging-
        # sensitive-data). Strength is enforced separately by
        # `make validate-secrets` before the stack comes up.
        sys.stderr.write(
            "WARNING: AIRFLOW_PASSWORD is one of the known-weak values; "
            "use a strong value in .env for non-local environments.\n"
        )

    # NOTE: Airflow 3's SimpleAuthManager does NOT support hashed passwords (like bcrypt).
    # It performs a direct plaintext string comparison. Storing it as a bcrypt hash will
    # cause authentication to fail (401 Unauthorized). Therefore, we must store the plaintext
    # password here. Since this file is located at a container-internal path that is never
    # mounted to the host, it remains secure from host-level leaks.
    payload = {user: password}
    OUTPUT_PATH.write_text(json.dumps(payload), encoding="utf-8")
    OUTPUT_PATH.chmod(stat.S_IRUSR | stat.S_IWUSR)  # 0600

    print(f"Wrote {OUTPUT_PATH} for user={user!r}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
