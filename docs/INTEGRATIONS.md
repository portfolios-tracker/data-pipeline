# Integration Guide

This document describes how the data-pipeline repository integrates with other services.

## Service Identity

- Service name: data-pipeline
- Repository: portfolios-tracker/data-pipeline
- Runtime: Apache Airflow + Python DAG modules

## Upstream and Downstream Dependencies

- Core API (NestJS): triggered via protected batch endpoints
- Supabase: persistent storage for market/enrichment data
- External providers: vnstock, yfinance, CoinGecko, Gemini (feature-dependent)

## Architecture Boundaries and Ownership

| Boundary | Owned by | Notes |
| --- | --- | --- |
| Airflow DAG declarations (`dags/*.py`) | Data Engineering | Scheduling, pools, dynamic mapping, callbacks, orchestrator invocation. |
| Orchestrators (`dags/etl_modules/orchestrators/`) | Data Engineering | Cross-layer control flow and partial/fatal failure semantics. |
| Adapters + transformers (`dags/etl_modules/adapters/`, `dags/etl_modules/transformers/`) | Data Engineering | Integration I/O boundaries + pure payload/row shaping logic. |
| Platform runtime/secrets (Airflow/Redis/Postgres infra) | Platform Engineering | Environment, deployment, credentials lifecycle. |
| Core API endpoint contracts | Core Backend + Data Engineering | Coordinate payload/schema changes before merge. |

## Core API Contract

Required environment variables:

- `NESTJS_API_URL`
- `DATA_PIPELINE_API_KEY`

Authentication header used by pipeline-triggered API calls:

- `X-Api-Key: <DATA_PIPELINE_API_KEY>`

## Change Management

1. Treat all exported tables and API-triggered payloads as versioned contracts.
2. Document schema-affecting changes with migration notes.
3. Coordinate contract changes with the core repository before merge.

## Boundary and Import Rules

Canonical rules live in [`docs/ARCHITECTURE.md`](./ARCHITECTURE.md) (source of truth).

Concise summary:

- Enforce directional flow: DAG modules -> orchestrator -> adapters + transformers.
- Use only `from dags.etl_modules...` imports for shared pipeline modules.
- Keep refactors incremental and preserve DAG parse health after each milestone (`airflow dags list`).

Import example:

```python
from dags.etl_modules.orchestrators.prices_orchestrator import process_price_chunk
```

Avoid legacy import roots like `from etl_modules...`.

## Integration Test Gates

When changing contracts or integration behavior, validate all three layers:

1. **Unit tests** (`./run_tests.sh --unit`) for adapters/transformers/orchestrators.
2. **Integration smoke** (`./run_tests.sh --integration`) to ensure DAG imports/parse still succeed.
3. **Failure-mode coverage** (`uv run pytest -k "partial_failures or raises_only_on_fatal_errors"`) for alert-vs-fatal behavior.
