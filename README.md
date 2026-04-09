# Portfolios Tracker: Data Pipeline

The **Data Pipeline** service is the institutional-grade ETL engine of the Portfolios Tracker platform. Built with **Apache Airflow**, it handles automated data ingestion, normalization, and Supabase-first market-data loading for multi-asset intelligence.

## Repository Scope

- Data Engineering owns DAG scheduling/execution and pipeline modules.
- Core API endpoint contracts are co-owned by Core Backend + Data Engineering.
- Platform Engineering owns Airflow/Redis/Postgres runtime and secrets lifecycle.
- Writes market and enrichment data into Supabase.

Operational docs:

- [Quick Start](./docs/QUICK-START.md)
- [Deployment](./docs/DEPLOYMENT.md)
- [Integrations](./docs/INTEGRATIONS.md)
- [Architecture](./docs/ARCHITECTURE.md)
- [DAG Ownership](./docs/DAG-OWNERSHIP.md)
- [Migration Playbook](./docs/MIGRATION-PLAYBOOK.md)
- [Troubleshooting](./docs/TROUBLESHOOTING.md)

## 🏗️ Architecture

The pipeline follows a modular ETL/ELT architecture:

- **Orchestrator**: Apache Airflow 3.x (CeleryExecutor)
- **Primary Storage**: Supabase Postgres (transactional data, market-data tables, embeddings, and summaries)
- **Message Broker**: Redis
- **Metadata DB**: PostgreSQL
- **Key Providers**: vnstock, yfinance, CoinGecko, Supabase

### Data Flow

```mermaid
graph LR
    A[External APIs] --> B[Airflow DAGs]
   B --> C[(Supabase Market Data)]
   B --> D[(Supabase App Data / pgvector)]
   B --> E[NestJS API]
```

## 📂 Core Components

- `dags/`: DAG definitions and task orchestration entrypoints.
- `dags/etl_modules/orchestrators/`: Workflow coordination across providers, transforms, and persistence.
- `dags/etl_modules/adapters/`: External/system-facing I/O boundaries (providers, notifications, repository adapters).
- `dags/etl_modules/transformers/`: Pure shaping/normalization logic for payloads and rows.
- `dags/etl_modules/settings.py`, `dags/etl_modules/errors.py`: Shared configuration and domain-level error contracts.

### Operating Model and Boundaries

- Directional flow is enforced: `DAG modules -> orchestrator -> adapters + transformers`.
- DAG files should stay thin: schedule + task wiring + orchestrator calls only.
- Orchestrators own control flow, retries, chunk/finalize behavior, and alert/fatal decisions.
- Adapters own side effects (API/database/notification calls). Transformers do not import adapters.
- Shared module imports use `from dags.etl_modules...` (see [`docs/ARCHITECTURE.md`](./docs/ARCHITECTURE.md)).

## ✅ Test Strategy

- **Unit tests** (`@pytest.mark.unit`): adapters, transformers, orchestrators, and error semantics in isolation.
  - Run: `./run_tests.sh --unit`
- **Integration tests** (`@pytest.mark.integration`): DAG import/parse smoke and end-to-end boundary wiring.
  - Run: `./run_tests.sh --integration`
- **Failure-mode tests** (primarily unit scope): partial failures, failed batches, and fatal error escalation in finalizers.
  - Run focused examples: `uv run pytest -k "partial_failures or raises_only_on_fatal_errors"`

## 🚀 Key Workflows (DAGs)

This table is a subset of primary workflows. For full DAG inventory (including `asset_promotion_check`), see [DAG Ownership](./docs/DAG-OWNERSHIP.md).

| DAG Name                      | Schedule           | Epic / Feature                             | Description                                                                    |
| :---------------------------- | :----------------- | :----------------------------------------- | :----------------------------------------------------------------------------- |
| `assets_dimension_etl`        | Weekly (Sun 2 AM)  | Epic 9.1 – Asset Dimensions                | Syncs asset master data (VN/US Stocks, Crypto, Precious Metals) into Supabase. |
| `market_data_prices_daily`    | Mon-Fri (6 PM ICT) | Epic 7 – EOD Market Data (Prices)          | Fetches and upserts end-of-day market prices.                                    |
| `market_data_events_daily`    | Mon-Fri (6:20 PM ICT) | Epic 7 – EOD Market Data (Events)      | Fetches and upserts daily corporate actions/events.                              |
| `market_data_ratios_weekly`   | Weekly (Sun 7 PM ICT) | Epic 7 – EOD Market Data (Ratios)      | Fetches and upserts financial ratios in weekly batches.                          |
| `market_data_fundamentals_weekly` | Weekly (Sun 7 PM ICT) | Epic 7 – EOD Market Data (Fundamentals) | Fetches and upserts income statements and balance sheets.                        |
| `refresh_historical_prices`   | Daily (6:30 PM ICT)| Epic 7 – Historical Refresh                | Rebuilds 6-year OHLCV history for assets affected by newly ingested corporate events. |
| `market_news_morning`         | Mon-Fri (7 AM ICT) | News Intelligence (Active)                 | Fetches VN stock news, stores in Supabase, sends AI summary to Telegram.       |
| `portfolio_schedule_snapshot` | Hourly (24/7)      | Epic 7 – Portfolio Tracking                | Triggers portfolio performance snapshots via NestJS API.                       |
| `ingest_company_intelligence` | Weekly (Sun 4 AM)  | Agentic Portfolio Creation – AI Embeddings | Ingests VN company profiles and upserts Gemini embeddings to pgvector.         |

### `market_news_morning` — Scope Decision

**Status:** ✅ **Retained** in active roadmap.

- **Product objective:** Deliver a curated, AI-powered morning news digest to users via Telegram before the VN market opens (9:15 AM ICT).
- **Success metrics:** Telegram delivery rate ≥ 99%; news freshness (last 24 h) ≥ 90% of items; Gemini summarisation latency ≤ 10 s.
- **Dependencies:** `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`, `GEMINI_API_KEY` in `.env`.

## 🛠️ Local Development

### Prerequisites

- Docker & Docker Compose
- `uv` (recommended for local Python environment)

### Setup

1. **Initialize Environment**:

   ```bash
   cp .env.example .env
   ```

2. **Start Cluster**:

   ```bash
   docker compose up -d
   ```

3. **Access UI**:
   - Airflow Webserver: [http://localhost:8080](http://localhost:8080) (default: `airflow`/`airflow`)
   - Flower (Celery Monitor): [http://localhost:5555](http://localhost:5555)
   - Required Airflow pools are bootstrapped automatically by the scheduler startup hook:
     - `vci_graphql`
     - `kbs_finance`

### Running Tests

```bash
./run_tests.sh
```

### Validation Gates

```bash
./scripts/validate.sh
```

- Default scope (`VALIDATE_SCOPE=phased`) enforces strict lint/type/test gates on migrated clean-architecture modules and their regression tests.
- Use `VALIDATE_SCOPE=full ./scripts/validate.sh` to run repo-wide strict validation.

## ⚙️ Configuration

Key environment variables in `.env`:

- `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID`: For alert notifications.
- `GEMINI_API_KEY`: For AI-powered news summarization.
- `DATA_PIPELINE_API_KEY`: Internal authentication for NestJS API calls.
- `SUPABASE_URL` / `SUPABASE_SECRET_OR_SERVICE_ROLE_KEY`: Supabase client access (supabase-py, asset lookups).
- `SUPABASE_DB_URL`: Direct Postgres connection string for bulk market data writes (psycopg2). Format: `postgresql://postgres:[password]@db.[project-ref].supabase.co:5432/postgres`.
- `AIRFLOW_POOL_VCI_GRAPHQL_SLOTS`: Slot count for `vci_graphql` pool (default: `8`).
- `AIRFLOW_POOL_KBS_FINANCE_SLOTS`: Slot count for `kbs_finance` pool (default: `8`).
