# Quick Start

## Prerequisites

- Docker and Docker Compose
- uv (optional for direct Python test execution)

## Local Setup

1. Create environment file:

```bash
cp .env.example .env
```

2. Start services:

```bash
docker compose up -d
```

3. Access Airflow UI:

- http://localhost:8080

Default local credentials are configured via environment variables.

## Validation

Run tests:

```bash
./run_tests.sh
```

Check DAG import health by opening Airflow UI and confirming DAG parse status.
