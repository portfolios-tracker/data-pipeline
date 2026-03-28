"""
data-pipeline/dags/refresh_adjusted_prices.py

Airflow DAG: refresh_adjusted_prices

Story 2.1 — Adjusted Market Data Pipeline

Task flow:
  fetch_tickers  →  ingest_vnindex  →  calculate_adjusted  →  upsert_adjusted_prices

Tickers source: Supabase market_data.assets WHERE asset_class='STOCK' AND market='VN'
Schedule: nightly at 18:30 Vietnam time (30 min after market_data_evening_batch at 18:00)
"""

import os
import sys
from datetime import date, datetime, timedelta

import pandas as pd
import psycopg2
import psycopg2.extras
from airflow import DAG
from airflow.sdk import task
from pendulum import timezone
from supabase import create_client

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl_modules.fetcher import fetch_index_history
from etl_modules.price_adjuster import calculate_adjusted_prices

SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")

local_tz = timezone("Asia/Bangkok")

# How many years of history to (re)calculate on each run.
HISTORY_YEARS = 6

VNINDEX_SYMBOL = "VNINDEX"


def _get_conn():
    """Open a psycopg2 connection. Always called inside a task to avoid
    module-level connection objects (consistent with data-pipeline pattern)."""
    if not SUPABASE_DB_URL:
        raise RuntimeError("SUPABASE_DB_URL environment variable is not set")
    return psycopg2.connect(SUPABASE_DB_URL)


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
    tags=["adjusted-prices", "supabase", "backtest"],
) as dag:

    @task
    def fetch_tickers_with_dividends():
        """
        Pull the full list of active VN stocks from Supabase market_data.assets table.
        Returns a list of ticker symbol strings.
        """
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_SECRET_OR_SERVICE_ROLE_KEY")
        if not url or not key:
            raise RuntimeError(
                "SUPABASE_URL or SUPABASE_SECRET_OR_SERVICE_ROLE_KEY not set"
            )
        client = create_client(url, key)
        response = (
            client.table("market_data.assets")
            .select("symbol")
            .eq("asset_class", "STOCK")
            .eq("market", "VN")
            .order("symbol")
            .execute()
        )
        tickers = [row["symbol"] for row in response.data if row.get("symbol")]
        print(f"Found {len(tickers)} active VN tickers in assets table")
        return tickers

    @task
    def ingest_vnindex():
        """
        Fetch VNINDEX history and upsert into market_data_prices so that
        refresh_adjusted_prices can also store an adjusted entry for it
        (treated as a zero-dividend index: adj_factor = 1.0).

        The builder uses close+volume and derived indicators; OHLC columns are
        intentionally excluded from the Supabase schema contract.
        """
        conn = _get_conn()
        try:
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
                "close",
                "volume",
                "source",
            ]
            df = df[[c for c in cols if c in df.columns]]
            df["trading_date"] = pd.to_datetime(df["trading_date"]).dt.date

            rows = [tuple(row) for row in df.itertuples(index=False, name=None)]
            present_cols = [c for c in cols if c in df.columns]
            updatable_cols = [
                c for c in present_cols if c not in {"ticker", "trading_date"}
            ]
            conflict_set_sql = ",\n                            ".join(
                [f"{col} = EXCLUDED.{col}" for col in updatable_cols]
                + ["ingested_at = NOW()"]
            )

            with conn:
                with conn.cursor() as cur:
                    psycopg2.extras.execute_values(
                        cur,
                        f"""
                        INSERT INTO market_data.market_data_prices
                            ({", ".join(present_cols)})
                        VALUES %s
                        ON CONFLICT (ticker, trading_date) DO UPDATE SET
                            {conflict_set_sql}
                        """,
                        rows,
                    )
            print(f"Upserted {len(df)} VNINDEX rows into market_data_prices")
        finally:
            conn.close()

    @task
    def calculate_adjusted(tickers: list[str]):
        """
        For each ticker (plus VNINDEX), fetch raw prices and dividend history
        from Supabase/Postgres, compute backward-adjusted prices, and return as
        a serialisable list of dicts for the upsert task.
        """
        conn = _get_conn()
        end_date = date.today().isoformat()
        start_date = (
            date.today().replace(year=date.today().year - HISTORY_YEARS).isoformat()
        )

        all_tickers = sorted(set(tickers + [VNINDEX_SYMBOL]))
        all_rows: list[dict] = []

        try:
            with conn.cursor() as cur:
                for ticker in all_tickers:
                    # --- Raw prices ---
                    cur.execute(
                        """
                        SELECT trading_date::text, close::text, volume::text
                        FROM market_data.market_data_prices
                        WHERE ticker = %s
                          AND trading_date BETWEEN %s::date AND %s::date
                        ORDER BY trading_date
                        """,
                        (ticker, start_date, end_date),
                    )
                    price_rows = cur.fetchall()
                    if not price_rows:
                        print(f"No price data for {ticker} — skipping adjustment")
                        continue

                    raw_prices = pd.DataFrame(
                        price_rows, columns=["trading_date", "close", "volume"]
                    )

                    # --- Dividends ---
                    if ticker == VNINDEX_SYMBOL:
                        dividends = pd.DataFrame(
                            columns=[
                                "exercise_date",
                                "cash_dividend_percentage",
                                "stock_dividend_percentage",
                            ]
                        )
                    else:
                        cur.execute(
                            """
                            SELECT exercise_date::text,
                                   cash_dividend_percentage::text,
                                   stock_dividend_percentage::text
                            FROM market_data.market_data_dividends
                            WHERE ticker = %s
                            ORDER BY exercise_date
                            """,
                            (ticker,),
                        )
                        div_rows = cur.fetchall()
                        dividends = pd.DataFrame(
                            div_rows,
                            columns=[
                                "exercise_date",
                                "cash_dividend_percentage",
                                "stock_dividend_percentage",
                            ],
                        )

                    adjusted_df = calculate_adjusted_prices(
                        raw_prices, dividends, ticker
                    )
                    if not adjusted_df.empty:
                        all_rows.extend(adjusted_df.to_dict(orient="records"))
        finally:
            conn.close()

        print(
            f"Calculated adjusted prices for {len(all_tickers)} tickers → {len(all_rows)} rows"
        )
        return all_rows

    @task
    def upsert_adjusted_prices(rows: list[dict]):
        """
        Bulk-upsert the calculated adjusted prices into market_data.adjusted_price_daily.
        Uses ON CONFLICT DO UPDATE for idempotent writes.
        """
        if not rows:
            print("No rows to upsert — nothing to do")
            return

        conn = _get_conn()
        df = pd.DataFrame(rows)

        # Ensure column types are JSON-serialisable for Airflow XCom (already done),
        # and cast to float for Postgres Numeric columns.
        df["adj_factor"] = df["adj_factor"].astype(float)
        df["adjusted_close"] = df["adjusted_close"].astype(float)
        df["adjusted_volume"] = df["adjusted_volume"].astype(float)
        df["raw_close"] = df["raw_close"].astype(float)

        insert_rows = list(
            df[
                [
                    "ticker",
                    "trading_date",
                    "adj_factor",
                    "adjusted_close",
                    "adjusted_volume",
                    "raw_close",
                ]
            ].itertuples(index=False, name=None)
        )

        try:
            with conn:
                with conn.cursor() as cur:
                    psycopg2.extras.execute_values(
                        cur,
                        """
                        INSERT INTO market_data.adjusted_price_daily
                            (ticker, trading_date, adj_factor, adjusted_close, adjusted_volume, raw_close)
                        VALUES %s
                        ON CONFLICT (ticker, trading_date) DO UPDATE SET
                            adj_factor       = EXCLUDED.adj_factor,
                            adjusted_close   = EXCLUDED.adjusted_close,
                            adjusted_volume  = EXCLUDED.adjusted_volume,
                            raw_close        = EXCLUDED.raw_close,
                            ingested_at      = NOW()
                        """,
                        insert_rows,
                    )
            print(f"Upserted {len(df)} rows into market_data.adjusted_price_daily")
        finally:
            conn.close()

    # DAG wiring
    tickers = fetch_tickers_with_dividends()
    vnindex_done = ingest_vnindex()
    # VNINDEX must be in market_data_prices before calculate_adjusted reads it
    vnindex_done >> tickers
    adjusted_rows = calculate_adjusted(tickers)
    upsert_adjusted_prices(adjusted_rows)
