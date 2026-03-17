# Troubleshooting

## Common Issues

### Airflow UI is up but DAGs are missing

- Verify DAG files mount correctly.
- Check container logs for import errors.
- Confirm dependencies are installed in runtime image.

### `portfolio_schedule_snapshot` fails with 401

- Ensure `DATA_PIPELINE_API_KEY` matches core API config.
- Verify header is sent as `X-Api-Key`.

### API call timeout errors

- Validate `NESTJS_API_URL` points to a reachable endpoint.
- Check network egress/firewall in deployment environment.
- Confirm core API health endpoint is reachable.

### Supabase write failures

- Verify `SUPABASE_DB_URL` and credentials.
- Check DB permissions and table existence.
- Inspect SQL/constraint errors in task logs.

### Test failures in CI/local

- Run `./run_tests.sh` for full suite.
- Run individual failing tests with `pytest -k <pattern>`.
- Ensure environment-dependent tests use mocks/fixtures.
