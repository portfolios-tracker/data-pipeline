from airflow import DAG
from airflow.sdk import TaskGroup, task
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from pendulum import timezone
import pandas as pd
import clickhouse_connect
import os
import sys

# Add dags directory to path so we can import etl_modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl_modules.fetcher import (
    fetch_stock_price,
    fetch_financial_ratios,
    fetch_dividends,
    fetch_income_stmt,
)
from etl_modules.notifications import (
    send_success_notification,
    send_failure_notification,
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
    tags=["stock-price", "financials", "clickhouse", "evening-batch"],
    on_success_callback=send_success_notification,
    on_failure_callback=send_failure_notification,
) as dag:
    # --- TASK GROUP 1: PRICES ---
    @task
    def extract_prices():
        price_data = []
        print("RUNNING DAILY INCREMENTAL (7 DAYS)")
        # Fetch 250 days for indicator calculation, but only keep last 7 days
        lookback_date = (datetime.today() - timedelta(days=250)).strftime("%Y-%m-%d")
        end_date = datetime.today().strftime("%Y-%m-%d")
        filter_from = (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")
        print(
            f"Fetching prices from {lookback_date} to {end_date} (will filter to last 7 days)"
        )

        for ticker in STOCKS:
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

        print(f"Connecting to ClickHouse at {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}...")
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
        )

        print(f"Inserting {len(data)} price rows...")
        price_cols = [
            "trading_date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "ticker",
            "daily_return",
            "ma_50",
            "ma_200",
            "rsi_14",
            "macd",
            "macd_signal",
            "macd_hist",
            "source",
        ]
        price_tuples = []
        for row in data:
            # Convert trading_date back to date object
            if row.get("trading_date"):
                row["trading_date"] = datetime.strptime(
                    row["trading_date"], "%Y-%m-%d"
                ).date()
            price_tuples.append([row.get(col) for col in price_cols])

        client.insert(
            "market_dwh.fact_stock_daily",
            price_tuples,
            column_names=price_cols,
        )
        print("Price insertion complete.")
        client.command("OPTIMIZE TABLE market_dwh.fact_stock_daily FINAL")

    # --- TASK GROUP 2: RATIOS ---
    @task
    def extract_ratios():
        ratio_data = []
        print("Fetching financial ratios...")
        for ticker in STOCKS:
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

        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
        )

        print(f"Inserting {len(data)} financial ratio rows...")
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
        ratio_tuples = []
        for row in data:
            # Convert fiscal_date back to date object
            if row.get("fiscal_date"):
                try:
                    row["fiscal_date"] = datetime.strptime(
                        row["fiscal_date"], "%Y-%m-%d"
                    ).date()
                except ValueError:
                    pass
            ratio_tuples.append([row.get(col, 0) for col in ratio_cols])

        client.insert(
            "market_dwh.fact_financial_ratios",
            ratio_tuples,
            column_names=ratio_cols,
        )
        print("Financial ratio insertion complete.")
        client.command("OPTIMIZE TABLE market_dwh.fact_financial_ratios FINAL")

    # --- TASK GROUP 3: FUNDAMENTALS (Dividends & Income Stmt) ---
    @task
    def extract_fundamentals():
        div_data = []
        income_data = []

        print("Fetching fundamentals (Dividends & Income Statements)...")
        for ticker in STOCKS:
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

        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
        )

        # Load Dividends
        divs = data.get("dividends", [])
        if divs:
            print(f"Inserting {len(divs)} dividend rows...")
            div_cols = [
                "ticker",
                "exercise_date",
                "cash_year",
                "cash_dividend_percentage",
                "stock_dividend_percentage",
                "issue_method",
            ]
            div_tuples = []
            for row in divs:
                # Convert exercise_date back to date object
                if row.get("exercise_date"):
                    row["exercise_date"] = datetime.strptime(
                        row["exercise_date"], "%Y-%m-%d"
                    ).date()
                div_tuples.append([row.get(col) for col in div_cols])

            client.insert(
                "market_dwh.fact_dividends",
                div_tuples,
                column_names=div_cols,
            )
            print("Dividend insertion complete.")
            client.command("OPTIMIZE TABLE market_dwh.fact_dividends FINAL")

        # Load Income Statement
        income = data.get("income_stmt", [])
        if income:
            print(f"Inserting {len(income)} income statement rows...")
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
            inc_tuples = []
            for row in income:
                # Convert fiscal_date back to date object
                if row.get("fiscal_date"):
                    row["fiscal_date"] = datetime.strptime(
                        row["fiscal_date"], "%Y-%m-%d"
                    ).date()
                # Handle None values for numeric columns (ClickHouse Decimal doesn't like None)
                vals = []
                for col in inc_cols:
                    val = row.get(col)
                    # If value is None and it's a metric column (not metadata), default to 0
                    if val is None and col not in [
                        "ticker",
                        "fiscal_date",
                        "year",
                        "quarter",
                    ]:
                        val = 0
                    vals.append(val)
                inc_tuples.append(vals)

            client.insert(
                "market_dwh.fact_income_statement",
                inc_tuples,
                column_names=inc_cols,
            )
            print("Income statement insertion complete.")
            client.command("OPTIMIZE TABLE market_dwh.fact_income_statement FINAL")

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
