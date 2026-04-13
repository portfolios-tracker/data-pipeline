# Migration Playbook: New DAG Checklist

Use this playbook when creating a new DAG or migrating legacy ETL logic into the current architecture model.

## Target Operating Model

Enforce this boundary flow:

`DAG modules -> orchestrator -> adapters + transformers`

- DAG files define schedule, task graph, callbacks, and call orchestrator entrypoints.
- Orchestrators coordinate chunking, retries, load/finalize flow, and alert-vs-fatal outcomes.
- Adapters own external I/O (providers, database, notifications).
- Transformers own deterministic shaping/normalization only.
- Shared settings/errors live in `dags/etl_modules/settings.py` and `dags/etl_modules/errors.py`.

## Import Convention (Mandatory)

All shared module imports must use the `dags.etl_modules.*` root.

```python
from dags.etl_modules.orchestrators.prices_orchestrator import process_price_chunk
```

Do not introduce `from etl_modules...` imports in new or migrated code.

## New DAG Checklist

- [ ] **Define ownership + contract**
  - Confirm primary owner (usually Data Engineering) and downstream contract owner(s).
  - If API payloads or table schemas change, coordinate with Core Backend/Platform before merge.

- [ ] **Create thin DAG entrypoint**
  - Add `dags/<dag_name>.py` with schedule, pools, callbacks, and dynamic mapping.
  - Keep business/data logic out of the DAG file.

- [ ] **Implement orchestrator-first flow**
  - Add/extend `dags/etl_modules/orchestrators/<domain>_orchestrator.py`.
  - Centralize chunk processing and finalization behavior.
  - Keep partial failures in alert mode; raise only on fatal errors.

- [ ] **Isolate adapters and transformers**
  - Put provider/database/notification integrations under `dags/etl_modules/adapters/`.
  - Put row/payload normalization under `dags/etl_modules/transformers/`.
  - Prevent adapter↔transformer direct coupling.

- [ ] **Wire settings, errors, and strict validation**
  - Reuse `settings.py` for tunables and environment-backed defaults.
  - Use `errors.py` (or domain-specific wrappers) for explicit failure categories.
  - Apply strict validation to required inputs/columns before write paths.

- [ ] **Add tests across three scopes**
  - Unit (`@pytest.mark.unit`): orchestrator, adapter, transformer, error mapping.
  - Integration (`@pytest.mark.integration`): DAG import + DagBag parse smoke.
  - Failure-mode: partial failures, failed batches, fatal escalation paths.

- [ ] **Update docs in same PR**
  - `README.md` (workflow + testing summary)
  - `docs/INTEGRATIONS.md` (contracts/boundaries)
  - `docs/DAG-OWNERSHIP.md` (new DAG row + owner)

## Validation Commands

Run from repo root:

```bash
./run_tests.sh --unit
./run_tests.sh --integration
uv run pytest -k "partial_failures or raises_only_on_fatal_errors"
```

If your DAG set changes, ensure expected DAG IDs/smoke checks are updated in integration tests.
