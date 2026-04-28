# Data Pipeline Context

## News Extractors

- **Disabled Sources:** VCI and KBS data sources for news are disabled.
  - **Reason:** VCI API is failing; KBS news are in Vietnamese (English required).
  - **Reference:** Removed from `services/data-pipeline/dags/etl_modules/extractors/factory.py` and `services/data-pipeline/dags/etl_modules/fetcher.py`.
