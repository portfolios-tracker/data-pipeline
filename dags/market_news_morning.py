import logging
import os
from datetime import datetime, timedelta

import pandas as pd
import psycopg2
import psycopg2.extras
from airflow import DAG
from airflow.sdk import task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from pendulum import timezone

from dags.etl_modules.extractors import run_all_extractors
from dags.etl_modules.fetcher import get_active_vn_stock_tickers
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
        tickers = get_active_vn_stock_tickers(raise_on_fallback=True)
        ticker_linked_rows, scraped_rows = run_all_extractors(tickers=tickers)
        logger.info(
            "extract_and_load_news: ticker_linked=%s scraped=%s",
            len(ticker_linked_rows),
            len(scraped_rows),
        )

        if not ticker_linked_rows and not scraped_rows:
            logger.info("No news to load.")
            return

        if not SUPABASE_DB_URL:
            raise RuntimeError("SUPABASE_DB_URL environment variable is not set")

        ticker_cols = [
            "asset_id",
            "news_id",
            "publish_date",
            "title",
            "news_content",
            "source",
            "source_url",
            "source_type",
        ]
        ticker_tuples = []
        for row in ticker_linked_rows:
            if row.get("publish_date"):
                row["publish_date"] = pd.to_datetime(row["publish_date"])
            ticker_tuples.append([row.get(c) for c in ticker_cols])

        scraped_cols = [
            "news_id",
            "publish_date",
            "title",
            "news_content",
            "source",
            "source_url",
            "source_type",
        ]
        scraped_tuples = []
        for row in scraped_rows:
            if row.get("publish_date"):
                row["publish_date"] = pd.to_datetime(row["publish_date"])
            scraped_tuples.append([row.get(c) for c in scraped_cols])

        logger.info(
            "extract_and_load_news: upserting ticker_linked=%s scraped=%s rows",
            len(ticker_tuples),
            len(scraped_tuples),
        )
        conn = psycopg2.connect(SUPABASE_DB_URL)
        try:
            with conn:
                with conn.cursor() as cur:
                    if ticker_tuples:
                        psycopg2.extras.execute_values(
                            cur,
                            """
                            INSERT INTO market_data.news
                                (asset_id, news_id, publish_date, title,
                                 news_content, source, source_url, source_type)
                            VALUES %s
                            ON CONFLICT (asset_id, news_id)
                            WHERE source_type = 'ticker_linked'
                            DO UPDATE SET
                                title        = EXCLUDED.title,
                                news_content = EXCLUDED.news_content,
                                source       = EXCLUDED.source,
                                source_url   = EXCLUDED.source_url,
                                publish_date = EXCLUDED.publish_date,
                                ingested_at  = NOW()
                            """,
                            ticker_tuples,
                        )

                    if scraped_tuples:
                        psycopg2.extras.execute_values(
                            cur,
                            """
                            INSERT INTO market_data.news
                                (news_id, publish_date, title, news_content,
                                 source, source_url, source_type)
                            VALUES %s
                            ON CONFLICT (news_id)
                            WHERE source_type = 'scraped'
                            DO UPDATE SET
                                title        = EXCLUDED.title,
                                news_content = EXCLUDED.news_content,
                                source       = EXCLUDED.source,
                                source_url   = EXCLUDED.source_url,
                                publish_date = EXCLUDED.publish_date,
                                ingested_at  = NOW()
                            """,
                            scraped_tuples,
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
