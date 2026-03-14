from airflow import DAG
from airflow.sdk import task
from datetime import datetime, timedelta
from pendulum import timezone
import pandas as pd
import psycopg2
import psycopg2.extras
import os
import sys

# Add dags directory to path so we can import etl_modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl_modules.fetcher import fetch_news, get_active_vn_stock_tickers
from etl_modules.notifications import (
    send_success_notification,
    send_failure_notification,
    send_telegram_news_summary,
)

SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")

default_args = {
    "owner": "data_engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# Set timezone to Vietnam (UTC+7)
local_tz = timezone("Asia/Bangkok")

with DAG(
    dag_id="market_news_morning",
    default_args=default_args,
    schedule="0 7 * * 1-5",  # 7 AM Vietnam Time Mon-Fri
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),
    catchup=False,
    tags=["news", "supabase", "morning-brief"],
    on_success_callback=send_success_notification,
    on_failure_callback=send_failure_notification,
) as dag:

    @task
    def extract_news():
        news_data = []
        tickers = get_active_vn_stock_tickers(raise_on_fallback=True)
        print(f"Fetching daily news for {len(tickers)} tickers...")

        for ticker in tickers:
            df = fetch_news(ticker)
            if not df.empty:
                news_data.append(df)

        if news_data:
            final_df = pd.concat(news_data)
            # Convert Timestamp objects to strings for JSON serialization
            if "publish_date" in final_df.columns:
                final_df["publish_date"] = final_df["publish_date"].astype(str)
            return final_df.to_dict("records")
        return []

    @task
    def load_news(data):
        if not data:
            print("No news data to load.")
            return

        if not SUPABASE_DB_URL:
            raise RuntimeError("SUPABASE_DB_URL environment variable is not set")

        print("Connecting to Supabase/Postgres...")
        conn = psycopg2.connect(SUPABASE_DB_URL)

        print(f"Inserting {len(data)} news rows...")
        cols = [
            "ticker",
            "publish_date",
            "title",
            "source",
            "price_at_publish",
            "price_change",
            "price_change_ratio",
            "rsi",
            "rs",
            "news_id",
        ]
        tuples = []
        for row in data:
            # Convert publish_date back to datetime object
            if row.get("publish_date"):
                # Handle potential different formats or just standard ISO
                try:
                    row["publish_date"] = pd.to_datetime(row["publish_date"])
                except Exception:
                    pass
            tuples.append([row.get(c) for c in cols])

        try:
            with conn:
                with conn.cursor() as cur:
                    psycopg2.extras.execute_values(
                        cur,
                        """
                        INSERT INTO public.market_data_news
                            (ticker, publish_date, title, source, price_at_publish,
                             price_change, price_change_ratio, rsi, rs, news_id)
                        VALUES %s
                        ON CONFLICT (ticker, news_id) DO UPDATE SET
                            publish_date = EXCLUDED.publish_date,
                            title = EXCLUDED.title,
                            source = EXCLUDED.source,
                            price_at_publish = EXCLUDED.price_at_publish,
                            price_change = EXCLUDED.price_change,
                            price_change_ratio = EXCLUDED.price_change_ratio,
                            rsi = EXCLUDED.rsi,
                            rs = EXCLUDED.rs,
                            ingested_at = NOW()
                        """,
                        tuples,
                    )
        finally:
            conn.close()

        print("News insertion complete.")

    @task
    def send_news_digest(data):
        """Send news summary to Telegram"""
        if data:
            send_telegram_news_summary(data)
            print(f"Sent {len(data)} news items to Telegram")
        else:
            print("No news to send")

    # Orchestration
    news_records = extract_news()
    load_news(news_records)
    send_news_digest(news_records)
