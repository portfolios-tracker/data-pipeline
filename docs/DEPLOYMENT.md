# Deployment Guide

## Deployment Topology

- data-pipeline: deployed independently (VPS/Coolify or equivalent)
- core API: external dependency reachable via `NESTJS_API_URL`
- Supabase: external dependency for storage

## Required Environment Variables

- `NESTJS_API_URL`
- `DATA_PIPELINE_API_KEY`
- `SUPABASE_URL`
- `SUPABASE_SECRET_OR_SERVICE_ROLE_KEY`
- `SUPABASE_DB_URL`
- Airflow security keys (`AIRFLOW__CORE__FERNET_KEY`, `AIRFLOW__API__SECRET_KEY`, `AIRFLOW__API_AUTH__JWT_SECRET`)

## Pre-Deployment Checklist

1. Verify API URL is reachable from deployment network.
2. Verify `DATA_PIPELINE_API_KEY` matches the core API configuration.
3. Verify Supabase connectivity from runtime container.
4. Run tests before deployment.

## Post-Deployment Checks

1. Airflow web UI is accessible.
2. DAGs are visible and parse successfully.
3. One manual trigger of `portfolio_schedule_snapshot` succeeds.
