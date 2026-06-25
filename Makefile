.PHONY: up down restart logs test lint fmt clean help init-secrets init-precommit security-scan validate-secrets

# ── Cross-platform / Windows (Git Bash) friendliness ──────────────────────────
# Run recipes under bash so the Unix tools the recipes call (sed/find/grep/awk/
# cp/rm) resolve. On Windows, run `make` from **Git Bash** — it ships make, bash,
# and those tools (PowerShell/cmd do not).
SHELL := bash
# uv must NOT re-sync the project env on every `uv run`: on a cloud-synced folder
# (OneDrive) the auto-sync re-copies files the sync daemon has locked and fails
# with `os error 32`. Provision the env once:
#   uv pip install -e ".[dev,airflow]" --link-mode=copy
# after that every `uv run` below skips the sync. UV_LINK_MODE=copy also makes any
# install copy instead of hardlink, dodging the cloud-folder hardlink error (396).
export UV_NO_SYNC := 1
export UV_LINK_MODE := copy

## —— Docker ——————————————————————————————————————————
up: validate-secrets ## Start all services (refuses to start on weak/placeholder creds)
	docker compose up --build -d

down: ## Stop all services and remove volumes
	docker compose down -v

restart: ## Restart all services
	docker compose restart

logs: ## Tail logs for all services
	docker compose logs -f

logs-airflow: ## Tail Airflow scheduler logs
	docker compose logs -f scheduler

logs-spark: ## Tail Spark master logs
	docker compose logs -f spark-master

## —— Quality ————————————————————————————————————————
test: ## Run tests with coverage
	uv run pytest --cov=src --cov-report=term-missing tests/

lint: ## Run ruff (check + format) and mypy — mirrors the CI Lint job exactly
	uv run ruff check src/ tests/ dags/
	uv run ruff format --check src/ tests/ dags/
	@uv run python -c "import mypy" 2>/dev/null || { echo "❌ mypy missing from the venv — run: uv pip install -e .[dev]"; exit 1; }
	uv run mypy src/

fmt: ## Auto-format code with ruff
	uv run ruff format src/ tests/ dags/
	uv run ruff check --fix src/ tests/ dags/

## —— Utilities ——————————————————————————————————————
clean: ## Remove generated files and caches
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .ruff_cache -exec rm -rf {} + 2>/dev/null || true
	rm -rf .coverage htmlcov/

fernet-key: ## Generate a new Fernet key for Airflow
	@uv run python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

webserver-key: ## Generate a new Webserver Secret Key for Airflow
	@uv run python -c "import secrets; print(secrets.token_urlsafe(32))"

jwt-secret-key: ## Generate a new JWT Secret Key for Airflow Execution API
	@uv run python -c "import secrets; print(secrets.token_hex(32))"

init-precommit: ## Install pre-commit + detect-secrets, then register Git hooks
	@uv pip install pre-commit detect-secrets
	@uv run pre-commit install
	@echo "✅ pre-commit hooks installed."
	@echo "   Hooks run automatically on 'git commit'. Use 'pre-commit run --all-files' to check the whole repo."

security-scan: ## Run pip-audit + check for known CVEs in dependencies (mirrors the CI Security job)
	@uv pip install "pip-audit>=2.7"
	@uv run pip-audit --strict --skip-editable

validate-secrets: ## Check .env credential strength (MinIO/Postgres/Dremio/Airflow) before `make up`
	@uv run python scripts/validate_secrets.py

init-secrets: ## Bootstrap airflow.env from .example with fresh Fernet + Webserver + JWT keys (idempotent: refuses to overwrite an existing airflow.env)
	@if [ -f airflow.env ]; then \
		echo "❌ airflow.env already exists — refusing to overwrite."; \
		echo "   Remove it manually and re-run if you really want fresh keys."; \
		exit 1; \
	fi
	@cp airflow.env.example airflow.env
	@FERNET_KEY=$$(uv run python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"); \
	WEBSERVER_KEY=$$(uv run python -c "import secrets; print(secrets.token_urlsafe(32))"); \
	JWT_KEY=$$(uv run python -c "import secrets; print(secrets.token_hex(32))"); \
	sed -i "s|your_generated_fernet_key_here|$$FERNET_KEY|g" airflow.env; \
	sed -i "s|your_generated_secret_key_here|$$WEBSERVER_KEY|g" airflow.env; \
	sed -i "s|your_generated_jwt_secret_key_here|$$JWT_KEY|g" airflow.env
	@echo "✅ airflow.env written with fresh Fernet + Webserver + JWT keys."
	@echo "   Remember to fill in MINIO_*, POSTGRES_*, DREMIO_* in .env."

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'
