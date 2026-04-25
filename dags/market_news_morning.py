import logging
import os
from datetime import datetime, timedelta

import pandas as pd
import psycopg2
import psycopg2.extras
from airflow import DAG
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from pendulum import timezone

from dags.etl_modules.extractors import run_all_extractors
from dags.etl_modules.notifications import (
    send_failure_notification,
    send_success_notification,
)

logger = logging.getLogger(__name__)

SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")

default_args = {
    "owner": "data_engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

local_tz = timezone("Asia/Bangkok")

with DAG(
    dag_id="market_news_morning",
    default_args=default_args,
    schedule="0 7 * * 1-5",  # 7 AM Vietnam Time Mon-Fri
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),
    catchup=False,
    tags=["news", "supabase", "morning-brief", "extraction"],
    on_success_callback=send_success_notification,
    on_failure_callback=send_failure_notification,
) as dag:

    @task
    def extract_and_load_news() -> None:
        """
        Extract news from all sources and load them directly into the database.
        """
        data = run_all_extractors()
        logger.info(
            "extract_and_load_news: %s records extracted from all sources", len(data)
        )

        if not data:
            logger.info("No news to load.")
            return

        if not SUPABASE_DB_URL:
            raise RuntimeError("SUPABASE_DB_URL environment variable is not set")

        cols = [
            "asset_id",
            "news_id",
            "publish_date",
            "title",
            "news_content",
            "source",
            "source_url",
        ]
        tuples = []
        for row in data:
            if row.get("publish_date"):
                row["publish_date"] = pd.to_datetime(row["publish_date"])
            tuples.append([row.get(c) for c in cols])

        logger.info(
            "extract_and_load_news: inserting %s rows into market_data.news",
            len(tuples),
        )
        conn = psycopg2.connect(SUPABASE_DB_URL)
        try:
            with conn:
                with conn.cursor() as cur:
                    psycopg2.extras.execute_values(
                        cur,
                        """
                        INSERT INTO market_data.news
                            (asset_id, news_id, publish_date, title,
                             news_content, source, source_url)
                        VALUES %s
                        ON CONFLICT (asset_id, news_id) DO UPDATE SET
                            title           = EXCLUDED.title,
                            news_content    = EXCLUDED.news_content,
                            ingested_at     = NOW()
                        """,
                        tuples,
                    )
        finally:
            conn.close()

        logger.info("extract_and_load_news: complete")

    extract = extract_and_load_news()
    
    trigger_scoring = TriggerDagRunOperator(
        task_id="trigger_scoring",
        trigger_dag_id="market_news_scoring",
        wait_for_completion=False,
    )

    trigger_embedding = TriggerDagRunOperator(
        task_id="trigger_embedding",
        trigger_dag_id="market_news_embedding",
        wait_for_completion=False,
    )

    extract >> [trigger_scoring, trigger_embedding]
