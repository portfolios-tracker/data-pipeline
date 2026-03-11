"""
Asset Promotion Check DAG
Checks asset_tracking_stats for assets eligible for pipeline promotion.
Promotes on-demand assets with sufficient user tracking to pipeline-managed tier.
Schedule: Daily 1 AM UTC
"""
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import logging
import psycopg2

logger = logging.getLogger(__name__)

PROMOTION_THRESHOLD = int(os.getenv("ASSET_PROMOTION_THRESHOLD", "3"))

default_args = {
    "owner": "data-pipeline",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "asset_promotion_check",
    default_args=default_args,
    description="Promote popular on-demand assets to pipeline-managed tier",
    schedule="0 1 * * *",
    catchup=False,
    tags=["assets", "promotion"],
)


def check_and_promote(**context):
    """Check asset_tracking_stats and promote eligible assets."""
    db_url = os.getenv("SUPABASE_DB_URL")
    if not db_url:
        raise RuntimeError("SUPABASE_DB_URL not set")

    with psycopg2.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE assets
                SET tracking_tier = 'pipeline_managed'
                WHERE id IN (
                    SELECT asset_id FROM asset_tracking_stats
                    WHERE user_count >= %s AND promotion_eligible = TRUE
                )
                AND tracking_tier = 'on_demand'
                RETURNING id, symbol
            """, (PROMOTION_THRESHOLD,))
            promoted = cur.fetchall()
            conn.commit()
            logger.info(f"Promoted {len(promoted)} assets to pipeline_managed")
            for asset_id, symbol in promoted:
                logger.info(f"  Promoted: {symbol} ({asset_id})")
    return len(promoted)


promote_task = PythonOperator(
    task_id="check_and_promote",
    python_callable=check_and_promote,
    dag=dag,
)
