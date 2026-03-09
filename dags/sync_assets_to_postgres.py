"""
Legacy Assets Sync DAG (deprecated)
Story: prep-3-seed-asset-data
Architecture: Single Table Inheritance (STI)

This DAG previously bridged ClickHouse ``dim_assets`` into Supabase ``assets``.
After Supabase-first migration, ``assets_dimension_etl`` writes directly to
Supabase, so this DAG is intentionally a no-op and retained temporarily to
avoid scheduler/registry churn during rollout.

Schedule: Nightly 3 AM
"""

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    "owner": "data-pipeline",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "sync_assets_to_postgres",
    default_args=default_args,
    description="Deprecated no-op: assets now write directly to Supabase",
    schedule="0 3 * * *",  # Daily 3 AM
    catchup=False,
    tags=["assets", "sync", "deprecated", "supabase"],
)


def sync_assets(**context):
    """
    Deprecated bridge task.

    Asset synchronization is now handled directly in assets_dimension_etl.
    """
    logger.info(
        "sync_assets_to_postgres is deprecated and now a no-op. "
        "assets_dimension_etl writes directly to Supabase assets."
    )
    return 0


# Define task
sync_task = PythonOperator(
    task_id="sync_assets_to_postgres",
    python_callable=sync_assets,
    dag=dag,
)
