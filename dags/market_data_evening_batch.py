from airflow import DAG
from airflow.sdk import TaskGroup, task
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from pendulum import timezone
import pandas as pd
import psycopg2
import psycopg2.extras
import os
import sys

# Add dags directory to path so we can import etl_modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl_modules.fetcher import (
    fetch_stock_price,
    fetch_financial_ratios,
    fetch_dividends,
    fetch_income_stmt,
    get_active_vn_stock_tickers,
)
from etl_modules.notifications import (
    send_success_notification,
    send_failure_notification,
)

# CONFIG
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")

default_args = {
    "owner": "data_engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

# Set timezone to Vietnam (UTC+7)
local_tz = timezone("Asia/Bangkok")

with DAG(
    dag_id="market_data_evening_batch",
    default_args=default_args,
    schedule="0 18 * * 1-5",  # 6 PM Vietnam Time Mon-Fri
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),
    catchup=False,
    tags=["stock-price", "financials", "supabase", "evening-batch"],
    on_success_callback=send_success_notification,
    on_failure_callback=send_failure_notification,
) as dag:
    # --- TASK GROUP 1: PRICES ---
    @task
    def extract_prices():
        price_data = []
        tickers = get_active_vn_stock_tickers(raise_on_fallback=True)
        print(f"RUNNING DAILY INCREMENTAL (7 DAYS) for {len(tickers)} tickers")
        # Fetch 250 days for indicator calculation, but only keep last 7 days
        lookback_date = (datetime.today() - timedelta(days=250)).strftime("%Y-%m-%d")
        end_date = datetime.today().strftime("%Y-%m-%d")
        filter_from = (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")
        print(
            f"Fetching prices from {lookback_date} to {end_date} (will filter to last 7 days)"
        )

        for ticker in tickers:
            df_price = fetch_stock_price(ticker, lookback_date, end_date)
            if not df_price.empty:
                # Filter to only last 7 days for insertion
                df_price = df_price[df_price["trading_date"].astype(str) >= filter_from]
                price_data.append(df_price)

        if price_data:
            final_price_df = pd.concat(price_data)
            # Convert date objects to strings for JSON serialization
            if "trading_date" in final_price_df.columns:
                final_price_df["trading_date"] = final_price_df["trading_date"].astype(
                    str
                )
            return final_price_df.to_dict("records")
        return []

    @task
    def load_prices(data):
        if not data:
            print("No price data to load.")
            return

        if not SUPABASE_DB_URL:
            raise RuntimeError("SUPABASE_DB_URL environment variable is not set")

        print(f"Connecting to Supabase/Postgres...")
        conn = psycopg2.connect(SUPABASE_DB_URL)

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
        for row in data:
            if row.get("trading_date"):
                row["trading_date"] = datetime.strptime(
                    row["trading_date"], "%Y-%m-%d"
                ).date()
            rows.append(tuple(row.get(col) for col in price_cols))

        print(f"Upserting {len(rows)} price rows into market_data_prices...")
        try:
            with conn:
                with conn.cursor() as cur:
                    psycopg2.extras.execute_values(
                        cur,
                        """
                        INSERT INTO market_data.market_data_prices
                            (trading_date, open, high, low, close, volume, ticker,
                             source)
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
            print("Price upsert complete.")
        finally:
            conn.close()

    # --- TASK GROUP 2: RATIOS ---
    @task
    def extract_ratios():
        ratio_data = []
        tickers = get_active_vn_stock_tickers(raise_on_fallback=True)
        print(f"Fetching financial ratios for {len(tickers)} tickers...")
        for ticker in tickers:
            df_ratio = fetch_financial_ratios(ticker)
            if not df_ratio.empty:
                ratio_data.append(df_ratio)

        if ratio_data:
            final_ratio_df = pd.concat(ratio_data)
            # Convert date objects to strings for JSON serialization
            if "fiscal_date" in final_ratio_df.columns:
                final_ratio_df["fiscal_date"] = final_ratio_df["fiscal_date"].astype(
                    str
                )
            return final_ratio_df.to_dict("records")
        return []

    @task
    def load_ratios(data):
        if not data:
            print("No ratio data to load.")
            return

        if not SUPABASE_DB_URL:
            raise RuntimeError("SUPABASE_DB_URL environment variable is not set")

        conn = psycopg2.connect(SUPABASE_DB_URL)

        ratio_cols = [
            "ticker",
            "fiscal_date",
            "year",
            "quarter",
            "pe_ratio",
            "pb_ratio",
            "ps_ratio",
            "p_cashflow_ratio",
            "eps",
            "bvps",
            "market_cap",
            "roe",
            "roa",
            "roic",
            "financial_leverage",
            "dividend_yield",
            "net_profit_margin",
            "debt_to_equity",
        ]

        rows = []
        for row in data:
            if row.get("fiscal_date"):
                try:
                    row["fiscal_date"] = datetime.strptime(
                        row["fiscal_date"], "%Y-%m-%d"
                    ).date()
                except ValueError:
                    pass
            rows.append(tuple(row.get(col, 0) for col in ratio_cols))

        print(f"Upserting {len(rows)} financial ratio rows into market_data_financial_ratios...")
        try:
            with conn:
                with conn.cursor() as cur:
                    psycopg2.extras.execute_values(
                        cur,
                        """
                        INSERT INTO market_data.market_data_financial_ratios
                            (ticker, fiscal_date, year, quarter, pe_ratio, pb_ratio,
                             ps_ratio, p_cashflow_ratio, eps, bvps, market_cap, roe,
                             roa, roic, financial_leverage, dividend_yield,
                             net_profit_margin, debt_to_equity)
                        VALUES %s
                        ON CONFLICT (ticker, fiscal_date) DO UPDATE SET
                            year              = EXCLUDED.year,
                            quarter           = EXCLUDED.quarter,
                            pe_ratio          = EXCLUDED.pe_ratio,
                            pb_ratio          = EXCLUDED.pb_ratio,
                            ps_ratio          = EXCLUDED.ps_ratio,
                            p_cashflow_ratio  = EXCLUDED.p_cashflow_ratio,
                            eps               = EXCLUDED.eps,
                            bvps              = EXCLUDED.bvps,
                            market_cap        = EXCLUDED.market_cap,
                            roe               = EXCLUDED.roe,
                            roa               = EXCLUDED.roa,
                            roic              = EXCLUDED.roic,
                            financial_leverage = EXCLUDED.financial_leverage,
                            dividend_yield    = EXCLUDED.dividend_yield,
                            net_profit_margin = EXCLUDED.net_profit_margin,
                            debt_to_equity    = EXCLUDED.debt_to_equity,
                            ingested_at       = NOW()
                        """,
                        rows,
                    )
            print("Financial ratio upsert complete.")
        finally:
            conn.close()

    # --- TASK GROUP 3: FUNDAMENTALS (Dividends & Income Stmt) ---
    @task
    def extract_fundamentals():
        div_data = []
        income_data = []
        tickers = get_active_vn_stock_tickers(raise_on_fallback=True)

        print(f"Fetching fundamentals (Dividends & Income Statements) for {len(tickers)} tickers...")
        for ticker in tickers:
            # Dividends
            df_div = fetch_dividends(ticker)
            if not df_div.empty:
                div_data.append(df_div)

            # Income Statement
            df_inc = fetch_income_stmt(ticker)
            if not df_inc.empty:
                income_data.append(df_inc)

        results = {}
        if div_data:
            df_div_final = pd.concat(div_data)
            # Convert date objects to strings for JSON serialization
            if "exercise_date" in df_div_final.columns:
                df_div_final["exercise_date"] = df_div_final["exercise_date"].astype(
                    str
                )
            results["dividends"] = df_div_final.to_dict("records")
        else:
            results["dividends"] = []

        if income_data:
            df_inc_final = pd.concat(income_data)
            # Convert date objects to strings for JSON serialization
            if "fiscal_date" in df_inc_final.columns:
                df_inc_final["fiscal_date"] = df_inc_final["fiscal_date"].astype(str)
            results["income_stmt"] = df_inc_final.to_dict("records")
        else:
            results["income_stmt"] = []

        return results

    @task
    def load_fundamentals(data):
        if not data:
            print("No fundamental data to load.")
            return

        if not SUPABASE_DB_URL:
            raise RuntimeError("SUPABASE_DB_URL environment variable is not set")

        conn = psycopg2.connect(SUPABASE_DB_URL)

        try:
            # Load Dividends
            divs = data.get("dividends", [])
            if divs:
                print(f"Upserting {len(divs)} dividend rows into corporate_actions...")
                div_cols = [
                    "ticker",
                    "exercise_date",
                    "cash_year",
                    "cash_dividend_percentage",
                    "stock_dividend_percentage",
                    "issue_method",
                ]
                div_rows = []
                for row in divs:
                    if row.get("exercise_date"):
                        row["exercise_date"] = datetime.strptime(
                            row["exercise_date"], "%Y-%m-%d"
                        ).date()
                    div_rows.append(tuple(row.get(col) for col in div_cols))

                with conn:
                    with conn.cursor() as cur:
                        psycopg2.extras.execute_values(
                            cur,
                            """
                            INSERT INTO market_data.corporate_actions
                                (ticker, exercise_date, cash_year,
                                 cash_dividend_percentage, stock_dividend_percentage,
                                 issue_method)
                            VALUES %s
                            ON CONFLICT (ticker, exercise_date) DO UPDATE SET
                                cash_year                   = EXCLUDED.cash_year,
                                cash_dividend_percentage    = EXCLUDED.cash_dividend_percentage,
                                stock_dividend_percentage   = EXCLUDED.stock_dividend_percentage,
                                issue_method                = EXCLUDED.issue_method,
                                ingested_at                 = NOW()
                            """,
                            div_rows,
                        )
                print("Dividend upsert complete.")

            # Load Income Statement
            income = data.get("income_stmt", [])
            if income:
                print(f"Upserting {len(income)} income statement rows into market_data_income_statements...")
                inc_cols = [
                    "ticker",
                    "fiscal_date",
                    "year",
                    "quarter",
                    "revenue",
                    "cost_of_goods_sold",
                    "gross_profit",
                    "operating_profit",
                    "net_profit_post_tax",
                ]
                inc_rows = []
                for row in income:
                    if row.get("fiscal_date"):
                        row["fiscal_date"] = datetime.strptime(
                            row["fiscal_date"], "%Y-%m-%d"
                        ).date()
                    # Postgres Numeric accepts None (NULL), no need to coerce to 0
                    inc_rows.append(tuple(row.get(col) for col in inc_cols))

                with conn:
                    with conn.cursor() as cur:
                        psycopg2.extras.execute_values(
                            cur,
                            """
                            INSERT INTO market_data.market_data_income_statements
                                (ticker, fiscal_date, year, quarter, revenue,
                                 cost_of_goods_sold, gross_profit, operating_profit,
                                 net_profit_post_tax)
                            VALUES %s
                            ON CONFLICT (ticker, fiscal_date) DO UPDATE SET
                                year                = EXCLUDED.year,
                                quarter             = EXCLUDED.quarter,
                                revenue             = EXCLUDED.revenue,
                                cost_of_goods_sold  = EXCLUDED.cost_of_goods_sold,
                                gross_profit        = EXCLUDED.gross_profit,
                                operating_profit    = EXCLUDED.operating_profit,
                                net_profit_post_tax = EXCLUDED.net_profit_post_tax,
                                ingested_at         = NOW()
                            """,
                            inc_rows,
                        )
                print("Income statement upsert complete.")
        finally:
            conn.close()

    # --- ORCHESTRATION WITH PARALLEL TASK GROUPS ---

    # GROUP 1: Stock Prices
    with TaskGroup("price_pipeline", tooltip="Daily Price Data") as price_group:
        e_price = extract_prices()
        l_price = load_prices(e_price)
        e_price >> l_price

    # GROUP 2: Financial Ratios
    with TaskGroup("ratio_pipeline", tooltip="Quarterly Ratios") as ratio_group:
        e_ratio = extract_ratios()
        l_ratio = load_ratios(e_ratio)
        e_ratio >> l_ratio

    # GROUP 3: Fundamentals (Dividends/Income)
    with TaskGroup("fundamental_pipeline", tooltip="Dividends & Income") as fund_group:
        e_fund = extract_fundamentals()
        l_fund = load_fundamentals(e_fund)
        e_fund >> l_fund

    # FINAL STEP: Data Quality Check or Notification
    notify_complete = EmptyOperator(task_id="pipeline_complete")

    # THE PARALLEL STRUCTURE
    [price_group, ratio_group, fund_group] >> notify_complete
