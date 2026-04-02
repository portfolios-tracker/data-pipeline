# DAG Ownership and Schedule

| DAG                         | Schedule               | Primary Domain          | Owner            |
| --------------------------- | ---------------------- | ----------------------- | ---------------- |
| assets_dimension_etl        | Weekly (Sun 2 AM ICT)  | Asset dimensions        | Data Engineering |
| market_data_evening_batch   | Weekdays (6 PM ICT)    | EOD market ingestion    | Data Engineering |
| refresh_adjusted_prices     | Weekdays (6:30 PM ICT) | Adjusted pricing        | Data Engineering |
| market_news_morning         | Weekdays (7 AM ICT)    | News + summarization    | Data Engineering |
| portfolio_schedule_snapshot | Hourly                 | API-triggered snapshots | Platform/Data    |
| ingest_company_intelligence | Weekly (Sun 4 AM ICT)  | Company intelligence    | Data + AI        |
| asset_promotion_check       | Daily (1 AM UTC)       | Asset tier promotion    | Data Engineering |

## Escalation

- First line: Data Engineering on-call
- Contract issues with API endpoints: Core backend owner
- Credential or provider failures: Platform owner
