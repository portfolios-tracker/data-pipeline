from airflow import DAG
from airflow.sdk import task
from datetime import datetime, timedelta
from pendulum import timezone
import pandas as pd
import clickhouse_connect
import os
import sys

# Add dags directory to path so we can import etl_modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl_modules.fetcher import fetch_news
from etl_modules.notifications import (
    send_success_notification,
    send_failure_notification,
    send_telegram_news_summary,
)

# CONFIG
STOCKS = ["HPG", "VCB", "VNM", "FPT", "MWG", "VIC"]
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse-server")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", 8123))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")

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
    tags=["news", "clickhouse", "morning-brief"],
    on_success_callback=send_success_notification,
    on_failure_callback=send_failure_notification,
) as dag:

    @task
    def extract_news():
        news_data = []
        print("Fetching daily news...")

        for ticker in STOCKS:
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

        print(f"Connecting to ClickHouse at {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}...")
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
        )

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

        client.insert("market_dwh.fact_news", tuples, column_names=cols)
        print("News insertion complete.")
        client.command("OPTIMIZE TABLE market_dwh.fact_news FINAL")

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
