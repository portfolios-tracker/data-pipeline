"""
services/data-pipeline/dags/refresh_adjusted_prices.py

Airflow DAG: refresh_adjusted_prices

Story 2.1 — Adjusted Market Data Pipeline

Task flow:
  fetch_tickers  →  ingest_vnindex  →  calculate_adjusted  →  upsert_adjusted_ohlcv

Tickers source: portfolios_tracker_dw.dim_assets WHERE asset_class='STOCK' AND market='VN' AND is_active=1
Schedule: nightly at 18:30 Vietnam time (30 min after market_data_evening_batch at 18:00)
"""

import os
import sys
from datetime import date, datetime, timedelta

import clickhouse_connect
import pandas as pd
from airflow import DAG
from airflow.sdk import task
from pendulum import timezone

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl_modules.fetcher import fetch_index_history
from etl_modules.price_adjuster import calculate_adjusted_prices

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse-server")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", 8123))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")

local_tz = timezone("Asia/Bangkok")

# How many years of history to (re)calculate on each run.
HISTORY_YEARS = 6

VNINDEX_SYMBOL = "VNINDEX"


def _get_client():
    """Instantiate a ClickHouse client. Always called inside a task to avoid
    module-level connection objects (consistent with market_data_evening_batch pattern)."""
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )


with DAG(
    dag_id="refresh_adjusted_prices",
    default_args={
        "owner": "data_engineer",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    schedule="30 18 * * 1-5",  # 30 min after market_data_evening_batch
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),
    catchup=False,
    tags=["adjusted-prices", "clickhouse", "backtest"],
) as dag:

    @task
    def fetch_tickers_with_dividends():
        """
        Pull the full list of active VN stocks from dim_assets.
        Returns a list of ticker symbol strings.
        """
        client = _get_client()
        result = client.query(
            """
            SELECT symbol
            FROM portfolios_tracker_dw.dim_assets FINAL
            WHERE asset_class = 'STOCK'
              AND market = 'VN'
              AND is_active = 1
            ORDER BY symbol
            """
        )
        tickers = [row[0] for row in result.result_rows]
        print(f"Found {len(tickers)} active VN tickers in dim_assets")
        return tickers

    @task
    def ingest_vnindex():
        """
        Fetch VNINDEX history and upsert into fact_stock_daily so that
        refresh_adjusted_prices can also store an adjusted entry for it
        (treated as a zero-dividend index: adj_factor = 1.0).
        """
        client = _get_client()
        end_date = date.today().isoformat()
        start_date = (
            date.today().replace(year=date.today().year - HISTORY_YEARS).isoformat()
        )

        df = fetch_index_history(VNINDEX_SYMBOL, start_date, end_date)
        if df.empty:
            print("WARNING: No VNINDEX data fetched — skipping upsert")
            return

        cols = [
            "ticker",
            "trading_date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "ma_50",
            "ma_200",
            "rsi_14",
            "daily_return",
            "macd",
            "macd_signal",
            "macd_hist",
            "source",
        ]
        df = df[[c for c in cols if c in df.columns]]
        client.insert_df("portfolios_tracker_dw.fact_stock_daily", df)
        print(f"Upserted {len(df)} VNINDEX rows into fact_stock_daily")

    @task
    def calculate_adjusted(tickers: list[str]):
        """
        For each ticker (plus VNINDEX), fetch raw prices and dividend history
        from ClickHouse, compute backward-adjusted prices, and return as a
        serialisable list of dicts for the upsert task.
        """
        client = _get_client()
        end_date = date.today().isoformat()
        start_date = (
            date.today().replace(year=date.today().year - HISTORY_YEARS).isoformat()
        )

        all_tickers = sorted(set(tickers + [VNINDEX_SYMBOL]))
        all_rows: list[dict] = []

        for ticker in all_tickers:
            # --- Raw prices ---
            price_result = client.query(
                """
                SELECT trading_date, close
                FROM portfolios_tracker_dw.fact_stock_daily FINAL
                WHERE ticker = {ticker:String}
                  AND trading_date BETWEEN {start_date:String} AND {end_date:String}
                ORDER BY trading_date
                """,
                parameters={
                    "ticker": ticker,
                    "start_date": start_date,
                    "end_date": end_date,
                },
            )
            if not price_result.result_rows:
                print(f"No price data for {ticker} — skipping adjustment")
                continue

            raw_prices = pd.DataFrame(
                price_result.result_rows, columns=["trading_date", "close"]
            )

            # --- Dividends ---
            if ticker == VNINDEX_SYMBOL:
                # Indices have no dividends; adjusted == raw
                dividends = pd.DataFrame(
                    columns=[
                        "exercise_date",
                        "cash_dividend_percentage",
                        "stock_dividend_percentage",
                    ]
                )
            else:
                div_result = client.query(
                    """
                    SELECT exercise_date, cash_dividend_percentage, stock_dividend_percentage
                    FROM portfolios_tracker_dw.fact_dividends FINAL
                    WHERE ticker = {ticker:String}
                    ORDER BY exercise_date
                    """,
                    parameters={"ticker": ticker},
                )
                dividends = pd.DataFrame(
                    div_result.result_rows,
                    columns=[
                        "exercise_date",
                        "cash_dividend_percentage",
                        "stock_dividend_percentage",
                    ],
                )

            adjusted_df = calculate_adjusted_prices(raw_prices, dividends, ticker)
            if not adjusted_df.empty:
                all_rows.extend(adjusted_df.to_dict(orient="records"))

        print(
            f"Calculated adjusted prices for {len(all_tickers)} tickers → {len(all_rows)} rows"
        )
        return all_rows

    @task
    def upsert_adjusted_ohlcv(rows: list[dict]):
        """
        Bulk-insert the calculated adjusted prices into portfolios_tracker_dw.adjusted_ohlcv.
        Uses ReplacingMergeTree idempotent semantics (ingested_at as version).
        """
        if not rows:
            print("No rows to upsert — nothing to do")
            return

        client = _get_client()
        df = pd.DataFrame(rows)

        # Ensure column types match ClickHouse schema
        df["adj_factor"] = df["adj_factor"].astype(float)
        df["adjusted_close"] = df["adjusted_close"].astype(float)
        df["raw_close"] = df["raw_close"].astype(
            float
        )  # ClickHouse driver handles Decimal64

        client.insert_df("portfolios_tracker_dw.adjusted_ohlcv", df)
        print(f"Upserted {len(df)} rows into portfolios_tracker_dw.adjusted_ohlcv")

    # DAG wiring
    tickers = fetch_tickers_with_dividends()
    vnindex_done = ingest_vnindex()
    # VNINDEX must be in fact_stock_daily before calculate_adjusted reads it
    vnindex_done >> tickers
    adjusted_rows = calculate_adjusted(tickers)
    upsert_adjusted_ohlcv(adjusted_rows)
