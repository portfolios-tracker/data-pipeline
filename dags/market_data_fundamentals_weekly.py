from airflow import DAG
from airflow.sdk import TaskGroup, task
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from itertools import islice
from pendulum import timezone
import pandas as pd
import psycopg2
import psycopg2.extras
import os

try:
    from etl_modules.fetcher import (
        fetch_balance_sheet,
        fetch_income_stmt,
        get_active_vn_stock_tickers,
    )
    from etl_modules.notifications import (
        send_success_notification,
        send_failure_notification,
    )
except ModuleNotFoundError as exc:
    if exc.name != "etl_modules":
        raise
    from dags.etl_modules.fetcher import (
        fetch_balance_sheet,
        fetch_income_stmt,
        get_active_vn_stock_tickers,
    )
    from dags.etl_modules.notifications import (
        send_success_notification,
        send_failure_notification,
    )

SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")
DB_UPSERT_BATCH_SIZE = int(os.getenv("DB_UPSERT_BATCH_SIZE", "100"))

default_args = {
    "owner": "data_engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

local_tz = timezone("Asia/Bangkok")


def _chunked_rows(rows, chunk_size):
    iterator = iter(rows)
    while True:
        chunk = list(islice(iterator, chunk_size))
        if not chunk:
            return
        yield chunk


def _to_payload(records, failed_symbols):
    return {
        "records": records,
        "failed_symbols": failed_symbols,
    }


def _unpack_payload(data):
    if isinstance(data, dict):
        return data.get("records", []), data.get("failed_symbols", [])
    if isinstance(data, list):
        return data, []
    return [], []


def _parse_date_value(value):
    if value in (None, "", "NaT", "nan"):
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    if isinstance(value, datetime):
        return value.date()
    if hasattr(value, "year") and hasattr(value, "month") and hasattr(value, "day"):
        return value
    return datetime.strptime(str(value), "%Y-%m-%d").date()


def _report_failed_symbols(stage, failed_symbols):
    if not failed_symbols:
        return
    print(f"{stage}: failed symbols ({len(failed_symbols)}):")
    for item in failed_symbols:
        print(f"- {item['symbol']}: {item['error']}")


def _upsert_rows_in_batches(conn, query, rows, *, table_name):
    if not rows:
        print(f"No rows to upsert for {table_name}.")
        return []

    failed_batches = []
    upserted_rows = 0
    total_batches = (len(rows) + DB_UPSERT_BATCH_SIZE - 1) // DB_UPSERT_BATCH_SIZE
    for batch_index, batch_rows in enumerate(
        _chunked_rows(rows, DB_UPSERT_BATCH_SIZE), start=1
    ):
        try:
            with conn:
                with conn.cursor() as cur:
                    psycopg2.extras.execute_values(cur, query, batch_rows)
            upserted_rows += len(batch_rows)
            print(
                f"{table_name}: upserted batch {batch_index}/{total_batches} "
                f"({len(batch_rows)} rows)"
            )
        except Exception as exc:
            conn.rollback()
            failed_batches.append(
                {"batch_index": batch_index, "size": len(batch_rows), "error": str(exc)}
            )
            print(
                f"{table_name}: batch {batch_index}/{total_batches} failed "
                f"({len(batch_rows)} rows): {exc}"
            )

    print(
        f"{table_name}: upsert summary {upserted_rows}/{len(rows)} rows, "
        f"failed_batches={len(failed_batches)}"
    )
    return failed_batches


with DAG(
    dag_id="market_data_fundamentals_weekly",
    default_args=default_args,
    schedule="0 19 * * 0",  # 7 PM Vietnam Time Sunday
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),
    catchup=False,
    tags=["financials", "supabase", "fundamentals", "weekly"],
    on_success_callback=send_success_notification,
    on_failure_callback=send_failure_notification,
) as dag:
    @task
    def extract_income_statements():
        income_data = []
        failed_symbols = []
        assets = get_active_vn_stock_tickers(raise_on_fallback=True)

        print(f"Fetching income statements for {len(assets)} tickers...")
        for asset in assets:
            symbol = asset["symbol"]
            try:
                df_inc = fetch_income_stmt(symbol, asset["asset_id"])
            except Exception as exc:
                failed_symbols.append({"symbol": symbol, "error": str(exc)})
                print(f"Income statement extraction failed for {symbol}: {exc}")
                continue
            if not df_inc.empty:
                income_data.append(df_inc)

        if income_data:
            df_inc_final = pd.concat(income_data, ignore_index=True)
            if "fiscal_date" in df_inc_final.columns:
                df_inc_final["fiscal_date"] = df_inc_final["fiscal_date"].astype(str)
            records = df_inc_final.to_dict("records")
        else:
            records = []
        _report_failed_symbols("extract_income_statements", failed_symbols)
        return _to_payload(records, failed_symbols)

    @task
    def load_income_statements(data):
        records, failed_symbols = _unpack_payload(data)
        _report_failed_symbols("load_income_statements (extract)", failed_symbols)
        if not records:
            print("No income statement data to load.")
            return {"failed_symbols": failed_symbols, "failed_rows": [], "failed_batches": []}

        if not SUPABASE_DB_URL:
            raise RuntimeError("SUPABASE_DB_URL environment variable is not set")

        conn = psycopg2.connect(SUPABASE_DB_URL)

        try:
            print(f"Upserting {len(records)} income statement rows into market_data.income_statements...")
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
            failed_rows = []
            for row in records:
                asset_id = row.get("asset_id")
                try:
                    row_values = dict(row)
                    row_values["fiscal_date"] = _parse_date_value(row.get("fiscal_date"))
                    inc_rows.append(tuple(row_values.get(col) for col in inc_cols))
                except Exception as exc:
                    failed_rows.append(
                        {
                            "symbol": str(asset_id or "unknown"),
                            "error": f"income-statement row conversion failed: {exc}",
                        }
                    )

            failed_batches = _upsert_rows_in_batches(
                conn,
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
                table_name="market_data.income_statements",
            )
            _report_failed_symbols("load_income_statements (row conversion)", failed_rows)
        finally:
            conn.close()
        return {
            "failed_symbols": failed_symbols,
            "failed_rows": failed_rows,
            "failed_batches": failed_batches,
        }

    @task
    def extract_balance_sheets():
        balance_data = []
        failed_symbols = []
        assets = get_active_vn_stock_tickers(raise_on_fallback=True)

        print(f"Fetching balance sheets for {len(assets)} tickers...")
        for asset in assets:
            symbol = asset["symbol"]
            try:
                df_bal = fetch_balance_sheet(symbol, asset["asset_id"])
            except Exception as exc:
                failed_symbols.append({"symbol": symbol, "error": str(exc)})
                print(f"Balance sheet extraction failed for {symbol}: {exc}")
                continue
            if not df_bal.empty:
                balance_data.append(df_bal)

        if balance_data:
            df_bal_final = pd.concat(balance_data, ignore_index=True)
            if "fiscal_date" in df_bal_final.columns:
                df_bal_final["fiscal_date"] = df_bal_final["fiscal_date"].astype(str)
            records = df_bal_final.to_dict("records")
        else:
            records = []
        _report_failed_symbols("extract_balance_sheets", failed_symbols)
        return _to_payload(records, failed_symbols)

    @task
    def load_balance_sheets(data):
        records, failed_symbols = _unpack_payload(data)
        _report_failed_symbols("load_balance_sheets (extract)", failed_symbols)
        if not records:
            print("No balance sheet data to load.")
            return {"failed_symbols": failed_symbols, "failed_rows": [], "failed_batches": []}

        if not SUPABASE_DB_URL:
            raise RuntimeError("SUPABASE_DB_URL environment variable is not set")

        conn = psycopg2.connect(SUPABASE_DB_URL)

        try:
            print(f"Upserting {len(records)} balance sheet rows into market_data.balance_sheets...")
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
            failed_rows = []
            for row in records:
                asset_id = row.get("asset_id")
                try:
                    row_values = dict(row)
                    row_values["fiscal_date"] = _parse_date_value(row.get("fiscal_date"))
                    bal_rows.append(tuple(row_values.get(col) for col in bal_cols))
                except Exception as exc:
                    failed_rows.append(
                        {
                            "symbol": str(asset_id or "unknown"),
                            "error": f"balance-sheet row conversion failed: {exc}",
                        }
                    )

            failed_batches = _upsert_rows_in_batches(
                conn,
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
                table_name="market_data.balance_sheets",
            )
            _report_failed_symbols("load_balance_sheets (row conversion)", failed_rows)
        finally:
            conn.close()
        return {
            "failed_symbols": failed_symbols,
            "failed_rows": failed_rows,
            "failed_batches": failed_batches,
        }

    with TaskGroup("fundamental_pipeline", tooltip="Income Statements") as fund_group:
        e_inc = extract_income_statements()
        l_inc = load_income_statements(e_inc)
        e_inc >> l_inc

    with TaskGroup("balance_sheet_group", tooltip="Quarterly Balance Sheets") as balance_group:
        e_bal = extract_balance_sheets()
        l_bal = load_balance_sheets(e_bal)
        e_bal >> l_bal

    notify_complete = EmptyOperator(task_id="pipeline_complete")

    [fund_group, balance_group] >> notify_complete
