---
name: airflow-dag-patterns
description: Build production Apache Airflow 3 DAGs with best practices for operators, assets, testing, and observability. Use when creating data pipelines, orchestrating workflows, or scheduling batch jobs.
---

# Apache Airflow 3 DAG Patterns

Production-ready patterns for Apache Airflow 3.x, featuring the new Task SDK, asset-based scheduling, and enhanced observability with OpenLineage.

## When to Use This Skill

- Creating data pipeline orchestration with Airflow 3
- Designing DAG structures using the new Task SDK (`airflow.sdk`)
- Implementing asset-based (data-driven) scheduling
- Ensuring DAG compatibility with Airflow 3 breaking changes
- Integrating OpenLineage for end-to-end data traceability
- Optimizing DAG performance by avoiding common parsing bottlenecks

## Core Concepts

### 1. DAG Design Principles

| Principle         | Description                                                                |
| ----------------- | -------------------------------------------------------------------------- |
| **Idempotent**    | Running a task twice with same inputs produces the same result.            |
| **Asset-Centric** | Schedule DAGs based on data availability (Assets) rather than time.        |
| **Stateless**     | Tasks should not rely on local file systems; use XCom or Assets for state. |
| **Observable**    | Automatic lineage tracking via OpenLineage integrated by default.          |

### 2. Task SDK Imports (Airflow 3.x)

Airflow 3 introduces a service-oriented architecture. Use the `airflow.sdk` for DAG authoring to decouple from the core Airflow services.

```python
# Preferred in Airflow 3.x
from airflow.sdk import DAG, task, Asset, AssetAlias
```

## Quick Start (Task SDK)

```python
# dags/example_v3_dag.py
import pendulum
from airflow.sdk import DAG, task, Asset
from airflow.operators.empty import EmptyOperator

# Define an Asset (replaces Dataset)
RAW_DATA = Asset("s3://data-lake/raw/source_records/{{ ds }}.json")

@dag(
    dag_id='source_ingestion_v3',
    schedule=[RAW_DATA], # Triggers when RAW_DATA is updated
    start_date=pendulum.datetime(2026, 1, 1),
    catchup=False,
    tags=['v3', 'ingestion'],
)
def ingestion_pipeline():

    @task(outlets=[Asset("mssql://raw/securities")])
    def process_to_mssql():
        """Process data and signal completion to downstream via outlet"""
        import pandas as pd
        # Logic here...
        print("Data processed to MSSQL")

    process_to_mssql()

ingestion_pipeline()
```

## Patterns

### Pattern 1: Advanced TaskFlow with Assets

```python
# dags/asset_driven_etl.py
from airflow.sdk import DAG, task, Asset

# Assets represent the state of data
MARKET_DATA = Asset("mssql://market/prices")

@dag(
    dag_id='market_analytics_v3',
    schedule=[MARKET_DATA],
    start_date=pendulum.datetime(2026, 1, 1),
    catchup=False,
)
def market_analytics():

    @task()
    def calculate_indicators() -> dict:
        # Local import for performance
        import pandas as pd
        return {"status": "success", "metrics": 42}

    @task()
    def load_mart(metrics: dict):
        print(f"Loading mart with {metrics}")

    load_mart(calculate_indicators())

market_analytics()
```

### Pattern 2: Dynamic Task Mapping

Useful for processing multiple entities (e.g., symbols) in parallel.

```python
@task
def get_symbols():
    return ["AAPL", "GOOGL", "MSFT"]

@task
def process_symbol(symbol):
    print(f"Processing {symbol}")

# Map the task to the list of symbols
symbols = get_symbols()
process_symbol.expand(symbol=symbols)
```

### Pattern 3: AssetAlias for Dynamic Data

`AssetAlias` allows resolving specific assets at runtime, enabling more dynamic pipelines.

```python
from airflow.sdk import Asset, AssetAlias

@task(outlets=[AssetAlias("daily_prices")])
def producer(*, outlet_events):
    # Resolve the alias to a specific asset instance
    current_asset = Asset(f"s3://bucket/prices_{pendulum.now().to_date_string()}")
    outlet_events[AssetAlias("daily_prices")].add(current_asset)

@dag(schedule=[AssetAlias("daily_prices")])
def consumer():
    ...
```

### Pattern 4: OpenLineage Integration

Ensure your SQL operators provide lineage information automatically by using compatible providers.

```python
# Airflow 3 uses common-sql with OpenLineage support
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

run_query = SQLExecuteQueryOperator(
    task_id="update_facts",
    conn_id="mssql_default",
    sql="EXEC usp_update_market_facts @batch_id='{{ params.batch_id }}'",
    # Lineage is captured automatically for supported dialects
)
```

## Best Practices

### Do's

- **Use `airflow.sdk`** - Future-proof your imports for Airflow 3+.
- **Avoid Top-Level Code** - Never perform heavy imports or database queries at the top level of a DAG file. Use local imports inside tasks.
- **Prefer Assets over Sensors** - Use `schedule=[Asset(...)]` to trigger DAGs based on data completion instead of polling with `ExternalTaskSensor`.
- **Use Ruff for Migration** - Run `ruff check dags/ --select AIR301` to find breaking changes when upgrading from Airflow 2.
- **Explicit Type Hints** - Use Python type hints for better XCom serialization and clarity.

### Don'ts

- **Don't use `Dataset`** - It is deprecated in favor of `Asset`.
- **Don't use `airflow.models.DAG`** - Use `airflow.sdk.DAG`.
- **Don't hardcode connections** - Use Airflow Connections and `conn_id`.
- **Don't put heavy logic in DAG files** - Keep the DAG definition clean; move logic to modules in `dags/sources/`.

## Observability & Traceability

Airflow 3 integrates **OpenLineage** by default. To maximize traceability:

1. Use `SQLExecuteQueryOperator` for all SQL tasks.
2. Define `outlets` for tasks that produce data.
3. Ensure the `common-sql` provider is installed with OpenLineage support:
   ```bash
   pip install "apache-airflow-providers-common-sql[openlineage]"
   ```

## Project Structure (Recommended)

```
dags/
├── common/             # Shared logic
├── sources/            # Source-specific logic by provider/system
│   ├── provider_a/
│   ├── provider_b/
│   └── provider_c/
├── sql/                # SQL scripts for your target warehouse/database
└── [dag_id].py         # DAG definitions using airflow.sdk
```
