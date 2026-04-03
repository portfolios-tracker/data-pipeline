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
    fetch_balance_sheet,
    fetch_corporate_events,
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
        assets = get_active_vn_stock_tickers(raise_on_fallback=True)
        print(f"RUNNING DAILY INCREMENTAL (7 DAYS) for {len(assets)} tickers")
        # Fetch 250 days for indicator calculation, but only keep last 7 days
        lookback_date = (datetime.today() - timedelta(days=250)).strftime("%Y-%m-%d")
        end_date = datetime.today().strftime("%Y-%m-%d")
        filter_from = (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")
        print(
            f"Fetching prices from {lookback_date} to {end_date} (will filter to last 7 days)"
        )

        for asset in assets:
            symbol = asset["symbol"]
            asset_id = asset["asset_id"]
            df_price = fetch_stock_price(symbol, asset_id, lookback_date, end_date)
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
            "asset_id",
            "source",
        ]

        rows = []
        for row in data:
            if row.get("trading_date"):
                row["trading_date"] = datetime.strptime(
                    row["trading_date"], "%Y-%m-%d"
                ).date()
            rows.append(tuple(row.get(col) for col in price_cols))

        print(f"Upserting {len(rows)} price rows into market_data.prices...")
        try:
            with conn:
                with conn.cursor() as cur:
                    psycopg2.extras.execute_values(
                        cur,
                        """
                        INSERT INTO market_data.prices
                            (trading_date, open, high, low, close, volume, asset_id,
                             source)
                        VALUES %s
                        ON CONFLICT (asset_id, trading_date) DO UPDATE SET
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
        assets = get_active_vn_stock_tickers(raise_on_fallback=True)
        print(f"Fetching financial ratios for {len(assets)} tickers...")
        for asset in assets:
            df_ratio = fetch_financial_ratios(asset["symbol"], asset["asset_id"])
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
            "asset_id",
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
            "current_ratio",
            "quick_ratio",
            "interest_coverage",
            "asset_turnover",
            "inventory_turnover",
            "receivable_turnover",
            "revenue_growth",
            "profit_growth",
            "operating_margin",
            "gross_margin",
            "free_cash_flow",
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

        print(f"Upserting {len(rows)} financial ratio rows into market_data.financial_ratios...")
        try:
            with conn:
                with conn.cursor() as cur:
                    psycopg2.extras.execute_values(
                        cur,
                        """
                        INSERT INTO market_data.financial_ratios
                            (asset_id, fiscal_date, year, quarter, pe_ratio, pb_ratio,
                             ps_ratio, p_cashflow_ratio, eps, bvps, market_cap, roe,
                             roa, roic, financial_leverage, dividend_yield,
                             net_profit_margin, debt_to_equity,
                             current_ratio, quick_ratio, interest_coverage,
                             asset_turnover, inventory_turnover, receivable_turnover,
                             revenue_growth, profit_growth, operating_margin,
                             gross_margin, free_cash_flow)
                        VALUES %s
                        ON CONFLICT (asset_id, fiscal_date) DO UPDATE SET
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
                            current_ratio     = EXCLUDED.current_ratio,
                            quick_ratio       = EXCLUDED.quick_ratio,
                            interest_coverage = EXCLUDED.interest_coverage,
                            asset_turnover    = EXCLUDED.asset_turnover,
                            inventory_turnover = EXCLUDED.inventory_turnover,
                            receivable_turnover = EXCLUDED.receivable_turnover,
                            revenue_growth    = EXCLUDED.revenue_growth,
                            profit_growth     = EXCLUDED.profit_growth,
                            operating_margin  = EXCLUDED.operating_margin,
                            gross_margin      = EXCLUDED.gross_margin,
                            free_cash_flow    = EXCLUDED.free_cash_flow,
                            ingested_at       = NOW()
                        """,
                        rows,
                    )
            print("Financial ratio upsert complete.")
        finally:
            conn.close()

    # --- TASK GROUP 3: FUNDAMENTALS ---
    @task
    def extract_income_statements():
        income_data = []
        assets = get_active_vn_stock_tickers(raise_on_fallback=True)

        print(f"Fetching income statements for {len(assets)} tickers...")
        for asset in assets:
            df_inc = fetch_income_stmt(asset["symbol"], asset["asset_id"])
            if not df_inc.empty:
                income_data.append(df_inc)

        if income_data:
            df_inc_final = pd.concat(income_data)
            if "fiscal_date" in df_inc_final.columns:
                df_inc_final["fiscal_date"] = df_inc_final["fiscal_date"].astype(str)
            return df_inc_final.to_dict("records")
        return []

    @task
    def load_income_statements(data):
        if not data:
            print("No income statement data to load.")
            return

        if not SUPABASE_DB_URL:
            raise RuntimeError("SUPABASE_DB_URL environment variable is not set")

        conn = psycopg2.connect(SUPABASE_DB_URL)

        try:
            print(f"Upserting {len(data)} income statement rows into market_data.income_statements...")
            inc_cols = [
                "asset_id",
                "fiscal_date",
                "year",
                "quarter",
                "revenue",
                "cost_of_goods_sold",
                "gross_profit",
                "operating_profit",
                "net_profit_post_tax",
                "selling_expenses",
                "admin_expenses",
                "financial_income",
                "financial_expenses",
                "other_income",
                "other_expenses",
                "ebitda",
            ]
            inc_rows = []
            for row in data:
                if row.get("fiscal_date"):
                    row["fiscal_date"] = datetime.strptime(
                        row["fiscal_date"], "%Y-%m-%d"
                    ).date()
                inc_rows.append(tuple(row.get(col) for col in inc_cols))

            with conn:
                with conn.cursor() as cur:
                    psycopg2.extras.execute_values(
                        cur,
                        """
                        INSERT INTO market_data.income_statements
                            (asset_id, fiscal_date, year, quarter, revenue,
                             cost_of_goods_sold, gross_profit, operating_profit,
                             net_profit_post_tax, selling_expenses, admin_expenses,
                             financial_income, financial_expenses, other_income,
                             other_expenses, ebitda)
                        VALUES %s
                        ON CONFLICT (asset_id, fiscal_date) DO UPDATE SET
                            year                = EXCLUDED.year,
                            quarter             = EXCLUDED.quarter,
                            revenue             = EXCLUDED.revenue,
                            cost_of_goods_sold  = EXCLUDED.cost_of_goods_sold,
                            gross_profit        = EXCLUDED.gross_profit,
                            operating_profit    = EXCLUDED.operating_profit,
                            net_profit_post_tax = EXCLUDED.net_profit_post_tax,
                            selling_expenses    = EXCLUDED.selling_expenses,
                            admin_expenses      = EXCLUDED.admin_expenses,
                            financial_income    = EXCLUDED.financial_income,
                            financial_expenses  = EXCLUDED.financial_expenses,
                            other_income        = EXCLUDED.other_income,
                            other_expenses      = EXCLUDED.other_expenses,
                            ebitda              = EXCLUDED.ebitda,
                            ingested_at         = NOW()
                        """,
                        inc_rows,
                    )
            print("Income statement upsert complete.")
        finally:
            conn.close()

    @task
    def extract_balance_sheets():
        balance_data = []
        assets = get_active_vn_stock_tickers(raise_on_fallback=True)

        print(f"Fetching balance sheets for {len(assets)} tickers...")
        for asset in assets:
            df_bal = fetch_balance_sheet(asset["symbol"], asset["asset_id"])
            if not df_bal.empty:
                balance_data.append(df_bal)

        if balance_data:
            df_bal_final = pd.concat(balance_data)
            if "fiscal_date" in df_bal_final.columns:
                df_bal_final["fiscal_date"] = df_bal_final["fiscal_date"].astype(str)
            return df_bal_final.to_dict("records")
        return []

    @task
    def load_balance_sheets(data):
        if not data:
            print("No balance sheet data to load.")
            return

        if not SUPABASE_DB_URL:
            raise RuntimeError("SUPABASE_DB_URL environment variable is not set")

        conn = psycopg2.connect(SUPABASE_DB_URL)

        try:
            print(f"Upserting {len(data)} balance sheet rows into market_data.balance_sheets...")
            bal_cols = [
                "asset_id",
                "fiscal_date",
                "year",
                "quarter",
                "total_assets",
                "total_liabilities",
                "total_equity",
                "cash_and_equivalents",
                "short_term_assets",
                "long_term_assets",
                "short_term_liabilities",
                "long_term_liabilities",
            ]
            bal_rows = []
            for row in data:
                if row.get("fiscal_date"):
                    row["fiscal_date"] = datetime.strptime(
                        row["fiscal_date"], "%Y-%m-%d"
                    ).date()
                bal_rows.append(tuple(row.get(col) for col in bal_cols))

            with conn:
                with conn.cursor() as cur:
                    psycopg2.extras.execute_values(
                        cur,
                        """
                        INSERT INTO market_data.balance_sheets
                            (asset_id, fiscal_date, year, quarter, total_assets,
                             total_liabilities, total_equity, cash_and_equivalents,
                             short_term_assets, long_term_assets, short_term_liabilities,
                             long_term_liabilities)
                        VALUES %s
                        ON CONFLICT (asset_id, fiscal_date) DO UPDATE SET
                            year                   = EXCLUDED.year,
                            quarter                = EXCLUDED.quarter,
                            total_assets           = EXCLUDED.total_assets,
                            total_liabilities      = EXCLUDED.total_liabilities,
                            total_equity           = EXCLUDED.total_equity,
                            cash_and_equivalents   = EXCLUDED.cash_and_equivalents,
                            short_term_assets      = EXCLUDED.short_term_assets,
                            long_term_assets       = EXCLUDED.long_term_assets,
                            short_term_liabilities = EXCLUDED.short_term_liabilities,
                            long_term_liabilities  = EXCLUDED.long_term_liabilities,
                            ingested_at            = NOW()
                        """,
                        bal_rows,
                    )
            print("Balance sheets upsert complete.")
        finally:
            conn.close()

    @task
    def extract_corporate_events():
        events_data = []
        assets = get_active_vn_stock_tickers(raise_on_fallback=True)

        print(f"Fetching corporate events for {len(assets)} tickers...")
        for asset in assets:
            df_ev = fetch_corporate_events(asset["symbol"], asset["asset_id"])
            if not df_ev.empty:
                events_data.append(df_ev)

        if events_data:
            df_ev_final = pd.concat(events_data)
            for dcol in ["event_date", "public_date", "exright_date"]:
                if dcol in df_ev_final.columns:
                    df_ev_final[dcol] = df_ev_final[dcol].astype(str)
            return df_ev_final.to_dict("records")
        return []

    @task
    def load_corporate_events(data):
        if not data:
            print("No corporate event data to load.")
            return

        if not SUPABASE_DB_URL:
            raise RuntimeError("SUPABASE_DB_URL environment variable is not set")

        conn = psycopg2.connect(SUPABASE_DB_URL)

        try:
            print(f"Upserting {len(data)} corporate event rows into market_data.corporate_events...")
            ev_cols = [
                "asset_id",
                "event_id",
                "event_date",
                "public_date",
                "exright_date",
                "event_title",
                "event_type",
                "event_description",
            ]
            ev_rows = []
            for row in data:
                for dcol in ["event_date", "public_date", "exright_date"]:
                    if row.get(dcol) and row[dcol] != 'NaT' and row[dcol] != 'nan':
                        row[dcol] = datetime.strptime(row[dcol], "%Y-%m-%d").date()
                    else:
                        row[dcol] = None
                ev_rows.append(tuple(row.get(col) for col in ev_cols))

            with conn:
                with conn.cursor() as cur:
                    psycopg2.extras.execute_values(
                        cur,
                        """
                        INSERT INTO market_data.corporate_events
                            (asset_id, event_id, event_date, public_date,
                             exright_date, event_title, event_type, event_description)
                        VALUES %s
                        ON CONFLICT (asset_id, event_id) DO UPDATE SET
                            event_date        = EXCLUDED.event_date,
                            public_date       = EXCLUDED.public_date,
                            exright_date      = EXCLUDED.exright_date,
                            event_title       = EXCLUDED.event_title,
                            event_type        = EXCLUDED.event_type,
                            event_description = EXCLUDED.event_description,
                            ingested_at       = NOW()
                        """,
                        ev_rows,
                    )
            print("Corporate events upsert complete.")
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

    # GROUP 3: Fundamentals
    with TaskGroup("fundamental_pipeline", tooltip="Income Statements") as fund_group:
        e_inc = extract_income_statements()
        l_inc = load_income_statements(e_inc)
        e_inc >> l_inc

    # GROUP 4: Balance Sheets
    with TaskGroup("balance_sheet_group", tooltip="Quarterly Balance Sheets") as balance_group:
        e_bal = extract_balance_sheets()
        l_bal = load_balance_sheets(e_bal)
        e_bal >> l_bal

    # GROUP 5: Corporate Events
    with TaskGroup("corporate_events_group", tooltip="Company Events") as events_group:
        e_ev = extract_corporate_events()
        l_ev = load_corporate_events(e_ev)
        e_ev >> l_ev

    # FINAL STEP: Data Quality Check or Notification
    notify_complete = EmptyOperator(task_id="pipeline_complete")

    # THE PARALLEL STRUCTURE
    [price_group, ratio_group, fund_group, balance_group, events_group] >> notify_complete
