# Architecture and Boundary Lock

This is the **source of truth** for dependency-direction and import-boundary rules in this repository.
If another document summarizes these rules, this file takes precedence.

## Dependency Direction

Required dependency flow:

`DAG modules -> orchestrator -> adapters + transformers`

Rules:

- DAG modules call orchestrator entrypoints.
- Orchestrator coordinates adapters and transformers.
- Transformers must not import adapter modules.
- Adapters must not import DAG modules.

## Adapter ↔ Transformer Interaction Policy

- Adapters and transformers are peer layers coordinated by orchestrator code.
- Direct imports between adapter modules and transformer modules are not allowed.
- Data should move between them through orchestrator-managed inputs/outputs.

## Import Policy

- Use one import style for shared modules: `from dags.etl_modules...`.
- Runtime fallback dual imports are not allowed in refactored code.
  - Disallowed pattern:
    - `try: from etl_modules...`
    - `except ModuleNotFoundError: from dags.etl_modules...`
- **Phased migration note:** a few legacy DAGs still use fallback imports until they are migrated
  (`assets_dimension_etl.py`, `ingest_company_intelligence.py`, `market_data_events_daily.py`,
  `market_news_morning.py`, `portfolio_schedule_snapshot.py`, `refresh_historical_prices.py`,
  and `etl_modules/vietstock_corp_actions.py`). New or refactored code must use the single
  absolute path policy immediately.
- **Runtime requirement:** Airflow containers must include `/opt/airflow` on `PYTHONPATH`
  (configured in `docker-compose.yml`) so `dags.etl_modules...` imports resolve during DAG parsing.

### Import Examples

Compliant:

```python
from dags.etl_modules.orchestrators.prices_orchestrator import process_price_chunk
```

Non-compliant:

```python
from etl_modules.market_data.orchestrator import run_prices_pipeline
```

## Incremental Rollout Constraint

- Refactor in small milestones.
- After each milestone, verify DAG parse remains green with `airflow dags list`.
- If parse safety regresses, stop and fix before continuing.
