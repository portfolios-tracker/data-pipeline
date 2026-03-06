# Data Pipeline Tasks

This task list tracks epic-alignment actions for `services/data-pipeline`.

## Priority 1 - Missing For Current Epics

- [ ] Implement Epic 9.1 precious metals ingestion (XAU/XAG) in `dags/assets_dimension_etl.py`.
- [ ] Define source mapping for metals symbols (for example `GC=F`, `SI=F`) and map to `asset_class=COMMODITY`.
- [ ] Ensure metals records are synced by `dags/sync_assets_to_postgres.py` and visible to API/web asset search.
- [ ] Add tests for metals ingestion and sync behavior in `tests/unit` and `tests/integration`.

## Priority 2 - Documentation And Alignment

- [ ] Keep `README.md` DAG inventory in sync with code when DAGs/schedules change.
- [ ] Align `docs/architecture-data-pipeline.md` with actual responsibilities (scheduler-only vs ETL + enrichment).
- [ ] Add epic coverage notes per DAG (Epic 7/9/thematic PoC) to reduce planning drift.

## Priority 3 - Scope Hygiene

- [ ] Decide whether `dags/market_news_morning.py` remains in active roadmap scope.
- [ ] If retained: map it to an explicit product objective and define success metrics.
- [ ] If not retained: deprecate with a clear migration/rollback note.

## Priority 4 - Operational Hardening

- [ ] Mark `scripts/manual_load_data.py` as local/dev-only and document safe usage boundaries.
- [ ] Review fallback ticker behavior in `dags/etl_modules/fetcher.py` to avoid silent partial ingestion in production.
- [ ] Add CI check that validates DAG list/schedule snippets in README against actual DAG definitions.
