# Portfolios Tracker: Data Pipeline

The **Data Pipeline** service is the institutional-grade ETL engine of the Portfolios Tracker platform. Built with **Apache Airflow**, it handles automated data ingestion, normalization, and Supabase-first market-data loading for multi-asset intelligence.

## Repository Scope

- Owns Airflow DAG scheduling and ETL execution.
- Integrates with the core NestJS API through protected batch endpoints.
- Writes market and enrichment data into Supabase.

Operational docs:

- [Quick Start](./docs/QUICK-START.md)
- [Deployment](./docs/DEPLOYMENT.md)
- [Integrations](./docs/INTEGRATIONS.md)
- [DAG Ownership](./docs/DAG-OWNERSHIP.md)
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

- `dags/`: Airflow directed acyclic graphs for all workflows.
- `dags/etl_modules/`: Shared logic for data fetching and notifications.

## 🚀 Key Workflows (DAGs)

| DAG Name                      | Schedule           | Epic / Feature                             | Description                                                                    |
| :---------------------------- | :----------------- | :----------------------------------------- | :----------------------------------------------------------------------------- |
| `assets_dimension_etl`        | Weekly (Sun 2 AM)  | Epic 9.1 – Asset Dimensions                | Syncs asset master data (VN/US Stocks, Crypto, Precious Metals) into Supabase. |
| `market_data_evening_batch`   | Mon-Fri (6 PM ICT) | Epic 7 – EOD Market Data                   | Fetches end-of-day prices, ratios, dividends, and income statements.           |
| `refresh_adjusted_prices`     | Mon-Fri (6:30 PM)  | Epic 7 / Story 2.1                         | Rebuilds backward-adjusted close and volume series for total-return backtests. |
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
