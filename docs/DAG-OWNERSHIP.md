# DAG Ownership and Schedule

| DAG                         | Schedule               | Primary Domain          | Owner            |
| --------------------------- | ---------------------- | ----------------------- | ---------------- |
| assets_dimension_etl        | Weekly (Sun 2 AM ICT)  | Asset dimensions        | Data Engineering |
| market_data_prices_daily    | Weekdays (6 PM ICT)    | EOD prices ingestion    | Data Engineering |
| market_data_events_daily    | Weekdays (6:20 PM ICT) | Corporate events ingestion | Data Engineering |
| market_data_ratios_weekly   | Weekly (Sun 7 PM ICT)  | Financial ratios ingestion | Data Engineering |
| market_data_fundamentals_weekly | Weekly (Sun 7 PM ICT) | Fundamentals ingestion | Data Engineering |
| refresh_historical_prices   | Daily (6:30 PM ICT)    | Historical price refresh| Data Engineering |
| market_news_morning         | Weekdays (7 AM ICT)    | News + summarization    | Data Engineering |
| portfolio_schedule_snapshot | Hourly                 | API-triggered snapshots | Platform Engineering + Data Engineering |
| ingest_company_intelligence | Weekly (Sun 4 AM ICT)  | Company intelligence    | Data Engineering + AI |
| asset_promotion_check       | Daily (1 AM UTC)       | Asset tier promotion    | Data Engineering |

## Architecture Boundary Ownership

| Layer / Area | Ownership | Responsibility Boundary |
| --- | --- | --- |
| `dags/*.py` (DAG declarations) | Data Engineering | Define schedules/tasks/callbacks; delegate business flow to orchestrators. |
| `dags/etl_modules/orchestrators/` | Data Engineering | Coordinate adapters + transformers; enforce partial-failure vs fatal-failure behavior. |
| `dags/etl_modules/adapters/` | Data Engineering | External side effects (providers, persistence, notifications). |
| `dags/etl_modules/transformers/` | Data Engineering | Deterministic data normalization and row shaping only. |
| `dags/etl_modules/settings.py`, `dags/etl_modules/errors.py` | Data Engineering | Centralized runtime config and domain error taxonomy. |
| Airflow runtime + secrets | Platform Engineering | Infra reliability, secret distribution, and runtime operations. |
| Core API endpoint contracts | Core Backend + Data Engineering | API payload/auth contract versioning and rollout coordination. |

## Escalation

- First line: Data Engineering on-call
- Contract issues with API endpoints: Core Backend owner
- Credential or provider failures: Platform Engineering owner

## Ownership Rules for New DAGs

- Keep DAG files thin and import shared modules only via `dags.etl_modules.*`.
- Place integration I/O in adapters and payload normalization in transformers.
- Put cross-step coordination and fatal/alert policy in orchestrators.
- Register ownership + schedule updates here in the same PR that introduces the DAG.
