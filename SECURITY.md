# Security policy

This is a portfolio / learning project, not a production service that holds
real user data. The threat model below reflects that, and so does the
disclosure process.

## Supported versions

| Version | Supported |
|---------|-----------|
| 3.2.x   | ✅ — current main branch |
| 2.x     | ❌ — superseded by 3.x; please rebase |

## Reporting a vulnerability

If you find something that looks like a real security issue (not a style
nit, not a Dependabot suggestion):

1. **Do not open a public GitHub issue.**
2. Email the maintainer (see the GitHub profile at
   <https://github.com/gustavolatorre>) with:
   - A short description of the problem
   - Steps to reproduce, ideally with `make up` from a clean state
   - The hash of the commit you tested against
3. Expect a first response within 7 days. If you don't hear back, ping
   publicly on the repo and reference your private message.

If the project gets enough traction to need a `security@` channel, we'll
move to GitHub's private security advisories.

## Scope

In scope (we care):
- Anything that could leak credentials from `.env` / `airflow.env` /
  `simple_auth_passwords.json` to a host or external party
- Anything that could let an unauthenticated network actor reach Spark,
  Nessie, MinIO, Postgres, or the Airflow execution API
- Anything that could let a privileged-but-not-root Airflow user execute
  arbitrary code outside the container
- Anything that lets a malicious dependency reach the warehouse via the
  `dbt build` task

Out of scope:
- Compromise of the host running `docker compose` — assumed trusted
- Vulnerabilities in `apache/spark:4.0.0`, `dremio/dremio-oss:25.0.0`, or
  other base images that are already fixed upstream and only require a
  bump (please open a Dependabot-style PR instead)
- Hardening of the SimpleAuthManager's plaintext password file — this is a
  known limitation of Airflow 3's bundled auth manager; replacing it with a
  real auth backend is a deliberately deferred item (see the roadmap below)

## Current hardening posture

What we already do:

| Control | Where |
|---------|-------|
| Rotated Fernet, API, and JWT secrets | `make init-secrets` |
| Credentials banned-list + min-length validator | `src/config/settings.py` |
| Docker images pinned to specific tags | `docker-compose.yml` |
| Nessie + Spark master not bound to host (`expose:` only) | `docker-compose.yml` |
| SHA-256 verification of the Python 3.12 tarball | `docker/Dockerfile.spark` |
| Dremio source provisioning logs scrubbed of credentials | `docker/dremio/setup_sources.sh` |
| Trivy fs **blocking gate on fixable HIGH + CRITICAL** + `pip-audit` & Trivy config (informational) on every PR | `.github/workflows/ci.yml` |
| `detect-secrets` + `detect-private-key` on every commit | `.pre-commit-config.yaml` |
| Trivy **image** scan of the built Spark/Airflow images | `.github/workflows/docker-build.yml` |
| CodeQL (Python SAST) on every push & PR | `.github/workflows/codeql.yml` |
| Dependabot version updates **+ security alerts & updates** for pip, GitHub Actions, Docker | `.github/dependabot.yml` |

What we still want (roadmap):

| Item | Status |
|------|--------|
| OIDC/OAuth login for Airflow (replace SimpleAuthManager) | Deferred |
| Bearer auth on Nessie REST API | Deferred |
| Encrypt secrets at rest (SOPS + age) instead of a plaintext gitignored `.env` | Deferred — see "Secrets at rest" below |
| Promote the remaining informational scans (`pip-audit`, Trivy config MEDIUM+) to blocking. The **HIGH+CRITICAL fs gate is now live** (promoted once the fixable HIGH backlog hit 0). **Promotion criterion:** flip each to blocking once it reports **0 new findings for 4 consecutive weekly runs** — the same objective bar that promoted the fs gate, so this actually happens instead of staying "informational" indefinitely. | Follow-up |

## Secrets at rest (deferred, with a chosen path)

Today every credential lives in a **gitignored plaintext `.env`** (+ `airflow.env`),
strength-gated by `make validate-secrets` and never committed (`detect-secrets`
runs on every commit). For a local-compose, loopback-only portfolio stack that is
adequate — the threat that a secrets manager closes (a secret leaking via the repo
or a shared host) is already covered by the gitignore + pre-commit + loopback
posture.

If this stack ever runs somewhere multi-tenant or off-box, the chosen path is
**SOPS + age**: commit an *encrypted* `.env` (decryptable only by holders of the
age key), so the secret material is versioned and rotatable without ever sitting
in plaintext at rest. It is deliberately **not** implemented now — half-built
crypto is worse than an honest, well-scoped plaintext file with a documented
upgrade path.

## Historical credential exposure (resolved)

Early in the project's history, `airflow.env` was tracked in git
(`8506a17`, `d57bc3a`, `afa068b`) before being removed in `3b878df` and added to
`.gitignore`. The file held Airflow **infrastructure signing keys**
(`AIRFLOW__CORE__FERNET_KEY`, the webserver/API `SECRET_KEY`, and the
`JWT_SECRET`) — not third-party or cloud credentials, and no customer data.

**Status: mitigated. History intentionally not rewritten.**

- **Rotated.** Every exposed key was rotated (logged in `CHANGELOG.md`); the values
  left in history are dead. (Verified: none of the historical secret values
  appear in the current `airflow.env`.)
- **No external surface.** Every service binds to `127.0.0.1` / the internal
  Docker network, so the keys grant no external access even setting rotation
  aside.
- **Recurrence blocked.** `airflow.env` is gitignored and `detect-secrets` +
  `detect-private-key` run on every commit (`.pre-commit-config.yaml`).

We deliberately did **not** `git filter-repo` the history: the residual risk is
effectively zero (dead, loopback-only infra keys), while rewriting a public
repo's history breaks every downstream commit SHA and existing PR reference for
a purely cosmetic gain — and transparency reads better than a silently-scrubbed
log. **This calculus flips** the moment a third-party / cloud credential (AWS
keys, API tokens, off-box DB passwords) lands in a commit: purge with
`git filter-repo` *and* rotate immediately.

## Disclosure timeline (target)

| Phase | Days from report |
|-------|------------------|
| First response | 7 |
| Triage + reproduction | 14 |
| Fix landed on `main` | 30 (negotiable for complex issues) |
| Public credit (with reporter's permission) | After release |
