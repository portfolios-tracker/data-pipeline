# Data Pipeline Tasks

This task list tracks epic-alignment actions for `services/data-pipeline`.

## Priority 1 - Missing For Current Epics

- [x] Implement Epic 9.1 precious metals ingestion (XAU/XAG) in `dags/assets_dimension_etl.py`.
- [x] Define source mapping for metals symbols (for example `GC=F`, `SI=F`) and map to `asset_class=COMMODITY`.
- [x] Ensure metals records are synced by `dags/sync_assets_to_postgres.py` and visible to API/web asset search.
- [x] Add tests for metals ingestion and sync behavior in `tests/unit` and `tests/integration`.

## Priority 2 - Documentation And Alignment

- [x] Keep `README.md` DAG inventory in sync with code when DAGs/schedules change.
- [x] Align `docs/architecture-data-pipeline.md` with actual responsibilities (ETL + enrichment, not scheduler-only).
- [x] Add epic coverage notes per DAG (Epic 7/9/Agentic Portfolio Creation) to reduce planning drift.

## Priority 3 - Scope Hygiene

- [x] Decide whether `dags/market_news_morning.py` remains in active roadmap scope.
- [x] Retained: mapped to product objective (AI morning brief) and success metrics documented in `README.md`.

## Priority 4 - Operational Hardening

- [x] Mark `scripts/manual_load_data.py` as local/dev-only and document safe usage boundaries.
- [x] Review fallback ticker behavior in `dags/etl_modules/fetcher.py` — added `raise_on_fallback` parameter; task callers now use `raise_on_fallback=True` to prevent silent partial ingestion in production.
- [x] Add CI check that validates DAG list/schedule snippets in README against actual DAG definitions (`scripts/validate_dag_registry.py` + CI step in `.github/workflows/test.yml`).
