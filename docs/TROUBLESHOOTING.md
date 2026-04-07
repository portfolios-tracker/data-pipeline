# Troubleshooting

## Common Issues

### Airflow UI is up but DAGs are missing

- Verify DAG files mount correctly.
- Check container logs for import errors.
- Confirm dependencies are installed in runtime image.

### Airflow task logs show `Invalid URL 'http://:8793/log/...'`

- **Symptom:** Task logs fail to open and show `Invalid URL ... No host supplied`.
- **Probable cause:** Airflow cannot resolve a worker hostname in Docker, so the log URL is generated with an empty host.
- **Config check:** In `docker-compose.yml`, ensure Airflow services include `AIRFLOW__CORE__HOSTNAME_CALLABLE: "airflow.utils.net.get_host_ip_address"` and keep worker log server port `8793`; then run `docker compose exec airflow-worker bash -lc 'echo $HOSTNAME'` and verify it is not empty.
- **Fix / restart:** Recreate Airflow services to refresh hostname/config (`make airflow-refresh`), then rerun the failed task and reopen logs.

### Airflow worker `PermissionError` on `/opt/airflow/logs`

- **Symptom:** Worker/task logs show `PermissionError: [Errno 13] Permission denied: '/opt/airflow/logs/...'` while creating task log directories.
- **Probable cause:** `./logs` is bind-mounted to `/opt/airflow/logs`, but host ownership/permissions do not match `AIRFLOW_UID` used by Airflow containers (`user: "${AIRFLOW_UID:-50000}:0"`).
- **Fix:**
  1. Confirm UID and current ownership:
     - `id -u`
     - `grep '^AIRFLOW_UID=' .env`
     - `stat -c '%u:%g %n' logs`
  2. Set `.env` `AIRFLOW_UID` to your host UID (usually `id -u`).
  3. Repair mounted directory permissions:
     - `sudo chown -R ${AIRFLOW_UID}:0 logs dags plugins config`
     - `sudo chmod -R u+rwX,g+rwX logs dags plugins config`
  4. Re-run init and recreate services:
     - `docker compose run --rm airflow-init`
     - `make airflow-refresh`
- **Verify:** `docker compose exec airflow-worker bash -lc 'id && ls -ld /opt/airflow/logs && touch /opt/airflow/logs/.perm_check && rm /opt/airflow/logs/.perm_check'` should succeed without `PermissionError`.

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
