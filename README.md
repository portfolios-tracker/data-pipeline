# Portfolios Tracker: Data Pipeline

The **Data Pipeline** service is the institutional-grade ETL engine of the Portfolios Tracker platform. Built with **Apache Airflow**, it handles automated data ingestion, normalization, and Supabase-first market-data loading for multi-asset intelligence.

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

- `dags/`: Airflow directed acyclic graphs for all workflows.
- `dags/etl_modules/`: Shared logic for data fetching and notifications.
- `scripts/`: Initialization and utility scripts.
- `sql/`: Reference SQL for market-data schemas and derived summary objects.

## 🚀 Key Workflows (DAGs)

| DAG Name                      | Schedule           | Epic / Feature                             | Description                                                                      |
| :---------------------------- | :----------------- | :----------------------------------------- | :------------------------------------------------------------------------------- |
| `assets_dimension_etl`        | Weekly (Sun 2 AM)  | Epic 9.1 – Asset Dimensions                | Syncs asset master data (VN/US Stocks, Crypto, Precious Metals) into Supabase.   |
| `market_data_evening_batch`   | Mon-Fri (6 PM ICT) | Epic 7 – EOD Market Data                   | Fetches end-of-day prices, ratios, dividends, and income statements.             |
| `refresh_adjusted_prices`     | Mon-Fri (6:30 PM)  | Epic 7 / Story 2.1                         | Rebuilds backward-adjusted close and volume series for total-return backtests.   |
| `market_news_morning`         | Mon-Fri (7 AM ICT) | News Intelligence (Active)                 | Fetches VN stock news, stores in Supabase, sends AI summary to Telegram.         |
| `portfolio_schedule_snapshot` | Hourly (24/7)      | Epic 7 – Portfolio Tracking                | Triggers portfolio performance snapshots via NestJS API.                         |
| `sync_assets_to_postgres`     | Daily (3 AM)       | Epic 9.1 – Asset Sync                      | Deprecated no-op retained temporarily to avoid scheduler churn during migration. |
| `ingest_company_intelligence` | Weekly (Sun 4 AM)  | Agentic Portfolio Creation – AI Embeddings | Ingests VN company profiles and upserts Gemini embeddings to pgvector.           |

### `market_news_morning` — Scope Decision

**Status:** ✅ **Retained** in active roadmap.

- **Product objective:** Deliver a curated, AI-powered morning news digest to users via Telegram before the VN market opens (9:15 AM ICT).
- **Success metrics:** Telegram delivery rate ≥ 99%; news freshness (last 24 h) ≥ 90% of items; Gemini summarisation latency ≤ 10 s.
- **Dependencies:** `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`, `GEMINI_API_KEY` in `.env`.
- **To deprecate:** Remove legacy warehouse terminology from the remaining news pipeline internals, keep Telegram integration optional, and archive this DAG only if the product direction changes. Open a tracking issue before proceeding.

## 🛠️ Local Development

### Prerequisites

- Docker & Docker Compose
- `uv` (recommended for local Python environment)

### Setup

1. **Initialize Environment**:

   ```bash
   cp template.env .env
   ```

2. **Start Cluster**:

   ```bash
   docker compose up -d
   ```

3. **Access UI**:
   - Airflow Webserver: [http://localhost:8080](http://localhost:8080) (default: `airflow`/`airflow`)
   - Flower (Celery Monitor): [http://localhost:5555](http://localhost:5555)

### Running Tests

```bash
./run_tests.sh
```

## ⚙️ Configuration

Key environment variables in `.env`:

- `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID`: For alert notifications.
- `GEMINI_API_KEY`: For AI-powered news summarization.
- `DATA_PIPELINE_API_KEY`: Internal authentication for NestJS API calls.
- `SUPABASE_URL` / `SUPABASE_SECRET_OR_SERVICE_ROLE_KEY`: Supabase client access (supabase-py, asset lookups).
- `SUPABASE_DB_URL`: Direct Postgres connection string for bulk market data writes (psycopg2). Format: `postgresql://postgres:[password]@db.[project-ref].supabase.co:5432/postgres`.

## 🔧 Developer Scripts

> ⚠️ **These scripts are for local development and manual data recovery ONLY.** Never run them in production without explicit approval — they perform direct market-data writes that can introduce duplicates or corrupt derived Supabase tables.

### `scripts/_archive/manual_load_data.py` (Archived)

Manually triggers the full ETL cycle (prices, ratios, dividends, income statements, news) for a configurable set of tickers and date range. Intended for:

- **Backfilling** historical data after a missed scheduled run.
- **Development/debugging** of ETL transformations.
- **Initial data seeding** in a fresh local environment.

**Usage:**

```bash
# Backfill a date range
uv run python scripts/_archive/manual_load_data.py --yes-really-run --start 2024-01-01 --end 2024-01-31

# Prices only
uv run python scripts/_archive/manual_load_data.py --yes-really-run --start 2024-01-01 --end 2024-01-31 --price-only

# Refresh company dimension from vnstock
uv run python scripts/_archive/manual_load_data.py --yes-really-run --update-companies
```

**Safe usage boundaries:**

1. Run from a local machine pointing to a **non-production** Supabase project or local Supabase stack.
2. Always pass `--yes-really-run` (script aborts without it).
3. Do not point recovery scripts at the production database without an approved rollback plan.
4. Do not run concurrently with a live Airflow worker processing the same tickers/date range.
5. The ticker list is hardcoded to `STOCKS = ["HPG", "VCB", "VNM", "FPT", "MWG", "VIC"]`; edit the file locally to expand it — do not commit those changes.

### Supabase schema rollout

Create or update the required Supabase schemas and tables with the repository migrations.

Legacy ClickHouse bootstrap artifacts are archived under `scripts/_archive/` and
`sql/_archive/` for historical reference only and are not part of active rollout.

There is no warehouse migration to run in this repository because no secondary analytical store was provisioned in the active environment. The rollout is to create the new Supabase market-data tables and point the ETL jobs and services at them directly.

### `scripts/validate_dag_registry.py`

CI utility that checks that every DAG defined in `dags/*.py` is documented in this `README.md`. Run locally or in CI to catch registry drift.

```bash
uv run python scripts/validate_dag_registry.py
```
