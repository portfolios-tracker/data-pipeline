# Copilot Instructions for `services/data-pipeline`

## Build, Run, and Test Commands

### Build / run stack
- Start local stack: `docker compose up -d`
- Start stack via Make target: `make airflow-data-up`
- Rebuild + recreate Airflow services after DAG/dependency/config changes: `make airflow-refresh`

### Test
- Full suite (recommended wrapper): `./run_tests.sh`
- Full suite (direct): `uv run pytest`
- Single test file: `./run_tests.sh -f tests/unit/test_fetcher.py`
- Single test case: `uv run pytest tests/unit/test_fetcher.py::TestCleanDecimalCols::test_replaces_nan_with_zero`
- Marker-based runs: `./run_tests.sh --unit` / `./run_tests.sh --integration` (or `uv run pytest -m unit`)
- Pattern targeting for failures: `uv run pytest -k <pattern>`

### Lint
- No dedicated lint command/target is defined in this repository (`Makefile` and `pyproject.toml` do not define one). Do not invent new lint tooling for routine edits.

## High-Level Architecture

- This service is an Airflow 3.x ETL orchestrator running with CeleryExecutor (Airflow API server, scheduler, worker, triggerer, dag-processor) backed by Redis (broker) and Postgres (Airflow metadata), defined in `docker-compose.yml`.
- DAG code lives in `dags/`; reusable logic lives in `dags/etl_modules/` (data providers, caching, request governance, notifications, refresh trigger helpers).
- Primary data sink is Supabase Postgres (`market_data.*` tables) written mostly through direct `psycopg2` bulk upserts (`execute_values`) using `SUPABASE_DB_URL`.
- The market-data EOD DAGs (prices/events/ratios/fundamentals) follow a common pattern: load active assets → chunk payloads → dynamic task mapping (`.expand`) per chunk → finalize task with `trigger_rule="all_done"` that aggregates partial failures and raises only on fatal chunk errors.
- `assets_dimension_etl` maintains the cross-asset master dimension in `market_data.assets` (STI-style discriminator via `asset_class` + `market`) from multiple providers (VCI, yfinance, CoinGecko).
- `portfolio_schedule_snapshot` is integration-facing: Airflow schedules; NestJS API executes batch snapshot logic, authenticated via `X-Api-Key: <DATA_PIPELINE_API_KEY>`.
- `refresh_historical_prices` depends on `market_data.corporate_events` changes to decide which assets require 6-year historical price backfills.

## Key Repository Conventions

- **Import compatibility pattern for DAG/runtime/test contexts:** modules commonly use:
  - `try: from etl_modules...`
  - `except ModuleNotFoundError: from dags.etl_modules...`
  Preserve this pattern when adding shared modules used both inside Airflow runtime and tests.
- **Test import style:** tests import shared helpers via `dags.etl_modules.*` paths (not ad-hoc `sys.path` hacks in normal test code).
- **Ticker universe filtering convention:** VN equity pipelines query active assets as `asset_class='STOCK'`, `market='VN'`, `status='active'`, and additionally filter out non-stock `metadata.symbol_type`.
- **Failure semantics in EOD loaders:** chunk tasks collect `failed_symbols` / row errors / failed batches; finalizer logs alert mode for partial failures but only hard-fails on fatal chunk errors to avoid replaying already-loaded chunks.
- **Callbacks/notifications:** DAGs generally wire `on_success_callback` and `on_failure_callback` to `etl_modules.notifications` Telegram-based callbacks.
- **Request throttling/retries:** provider calls should use `request_governor.governed_call(...)` for retry/backoff and Redis-aware rate limiting (with local fallback when Redis is unavailable).
- **Pytest marker strictness:** `pytest.ini` enables `--strict-markers`; only use declared markers (`unit`, `integration`, `slow`, `api`).
- **Skill-aware workflow:** if Copilot CLI skills/plugins are available in the session, invoke relevant skills first (especially process skills such as debugging, planning, and testing) before making implementation changes.
