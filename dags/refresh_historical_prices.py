import os
from datetime import datetime, timedelta

import pandas as pd
import psycopg2
import psycopg2.extras
from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import task
from pendulum import timezone

try:
    from etl_modules.fetcher import fetch_stock_price
    from etl_modules.notifications import (
        send_failure_notification,
        send_success_notification,
    )
except ModuleNotFoundError as exc:
    if exc.name != "etl_modules":
        raise
    from dags.etl_modules.fetcher import fetch_stock_price
    from dags.etl_modules.notifications import (
        send_failure_notification,
        send_success_notification,
    )

# CONFIG
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": send_failure_notification,
}

with DAG(
    "refresh_historical_prices",
    default_args=default_args,
    description="Refreshes full historical prices (OHLCV) when new corporate actions are detected",
    schedule="30 11 * * *",  # 18:30 VN time (UTC+7)
    start_date=datetime(2025, 1, 1, tzinfo=timezone("UTC")),
    catchup=False,
    tags=["market_data", "maintenance"],
    on_success_callback=send_success_notification,
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    @task
    def get_unprocessed_tickers():
        print("Connecting to Supabase to find unprocessed corporate actions...")
        conn = psycopg2.connect(SUPABASE_DB_URL)
        tickers = []
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT DISTINCT ticker 
                        FROM market_data.corporate_actions 
                        WHERE processed_at IS NULL
                        """
                    )
                    rows = cur.fetchall()
                    tickers = [row[0] for row in rows]
        finally:
            conn.close()

        print(f"Found {len(tickers)} tickers with unprocessed corporate actions.")
        return tickers

    @task
    def refresh_ticker_history(tickers: list):
        if not tickers:
            print("No tickers to process. Exiting.")
            return

        end_date = datetime.today().strftime("%Y-%m-%d")
        start_date = (datetime.today() - timedelta(days=365 * 6)).strftime("%Y-%m-%d")
        print(
            f"Fetching 6-year history ({start_date} to {end_date}) for tickers: {tickers}"
        )

        conn = psycopg2.connect(SUPABASE_DB_URL)
        try:
            for ticker in tickers:
                print(f"Processing {ticker}...")
                df_price = fetch_stock_price(ticker, start_date, end_date)

                if df_price.empty:
                    print(f"Warning: No data fetched for {ticker}. Skipping.")
                    continue

                # Prepare for bulk upsert
                price_cols = [
                    "trading_date",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "ticker",
                    "source",
                ]

                rows = []
                for _, row in df_price.iterrows():
                    # Handle Pandas Timestamp conversion
                    if isinstance(row.get("trading_date"), pd.Timestamp):
                        row["trading_date"] = row["trading_date"].date()
                    rows.append(tuple(row.get(col) for col in price_cols))

                print(f"Upserting {len(rows)} rows for {ticker}...")
                with conn:
                    with conn.cursor() as cur:
                        psycopg2.extras.execute_values(
                            cur,
                            """
                            INSERT INTO market_data.market_data_prices
                                (trading_date, open, high, low, close, volume, ticker, source)
                            VALUES %s
                            ON CONFLICT (ticker, trading_date) DO UPDATE SET
                                open          = EXCLUDED.open,
                                high          = EXCLUDED.high,
                                low           = EXCLUDED.low,
                                close         = EXCLUDED.close,
                                volume        = EXCLUDED.volume,
                                source        = EXCLUDED.source,
                                ingested_at   = NOW()
                            """,
                            rows,
                        )

                        # Mark corporate actions as processed
                        cur.execute(
                            """
                            UPDATE market_data.corporate_actions
                            SET processed_at = NOW()
                            WHERE ticker = %s AND processed_at IS NULL
                            """,
                            (ticker,),
                        )
                        print(f"Marked corporate actions for {ticker} as processed.")
        finally:
            conn.close()

    # Define execution graph
    tickers_to_process = get_unprocessed_tickers()
    refresh_task = refresh_ticker_history(tickers_to_process)

    start >> tickers_to_process >> refresh_task >> end
