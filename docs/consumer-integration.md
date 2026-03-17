# Consumer Integration Guide

This document describes how services integrate with data-pipeline.

## Service Identity

- Service name: data-pipeline
- Repository: portfolios-tracker/data-pipeline
- Runtime: Python and Airflow DAG modules

## Integration Modes

- Scheduled DAG execution for periodic jobs
- API-triggered batch workflows where applicable

## Environment and Auth

Use environment-scoped credentials for downstream systems:

- Supabase connection credentials
- Redis or cache settings (if enabled)
- DATA_PIPELINE_API_KEY when calling protected API batch endpoints

## Upstream or Downstream Contract

- Data consumers should treat pipeline outputs as versioned interfaces.
- Any schema-affecting change must include migration notes.
- Keep decimal precision policies aligned with API and storage contracts.

## Local Run

- Install dependencies with uv.
- Execute tests with ./run_tests.sh or pytest.
- Run DAG modules in a local orchestration environment for validation.

## CI Recommendations

- Run unit and integration tests on every pull request.
- Validate critical DAG import paths and job entrypoints.
- Block merges on breaking output schema changes unless migration steps are documented.
