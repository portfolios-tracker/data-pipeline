from airflow import DAG
from airflow.sdk import task
from datetime import datetime, timedelta
from itertools import islice
import math
from pendulum import timezone
import pandas as pd
import psycopg2
import psycopg2.extras
import os

try:
    from etl_modules.fetcher import fetch_financial_ratios, get_active_vn_stock_tickers
    from etl_modules.notifications import (
        send_success_notification,
        send_failure_notification,
    )
except ModuleNotFoundError as exc:
    if exc.name != "etl_modules":
        raise
    from dags.etl_modules.fetcher import fetch_financial_ratios, get_active_vn_stock_tickers
    from dags.etl_modules.notifications import (
        send_success_notification,
        send_failure_notification,
    )

# CONFIG
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")
DB_UPSERT_BATCH_SIZE = int(os.getenv("DB_UPSERT_BATCH_SIZE", "100"))
NUMERIC_SANITIZE_COLUMNS = (
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
)

default_args = {
    "owner": "data_engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

# Set timezone to Vietnam (UTC+7)
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


def _sanitize_numeric_value(value):
    if value in (None, "", "NaT", "nan"):
        return None, False
    try:
        if pd.isna(value):
            return None, False
    except TypeError:
        pass
    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        return value, False
    if not math.isfinite(numeric_value):
        return None, True
    return numeric_value, False


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
    dag_id="market_data_ratios_weekly",
    default_args=default_args,
    schedule="0 19 * * 0",  # 7 PM Vietnam Time Sunday
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),
    catchup=False,
    tags=["financials", "ratios", "supabase", "weekly"],
    on_success_callback=send_success_notification,
    on_failure_callback=send_failure_notification,
) as dag:
    @task
    def extract_ratios():
        ratio_data = []
        failed_symbols = []
        assets = get_active_vn_stock_tickers(raise_on_fallback=True)
        print(f"Fetching financial ratios for {len(assets)} tickers...")
        for asset in assets:
            symbol = asset["symbol"]
            try:
                df_ratio = fetch_financial_ratios(symbol, asset["asset_id"])
            except Exception as exc:
                failed_symbols.append({"symbol": symbol, "error": str(exc)})
                print(f"Ratio extraction failed for {symbol}: {exc}")
                continue
            if not df_ratio.empty:
                ratio_data.append(df_ratio)

        if ratio_data:
            final_ratio_df = pd.concat(ratio_data, ignore_index=True)
            # Convert date objects to strings for JSON serialization
            if "fiscal_date" in final_ratio_df.columns:
                final_ratio_df["fiscal_date"] = final_ratio_df["fiscal_date"].astype(str)
            records = final_ratio_df.to_dict("records")
        else:
            records = []
        _report_failed_symbols("extract_ratios", failed_symbols)
        return _to_payload(records, failed_symbols)

    @task
    def load_ratios(data):
        records, failed_symbols = _unpack_payload(data)
        _report_failed_symbols("load_ratios (extract)", failed_symbols)
        if not records:
            print("No ratio data to load.")
            return {"failed_symbols": failed_symbols, "failed_rows": [], "failed_batches": []}

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
        failed_rows = []
        non_finite_count = 0
        for row in records:
            asset_id = row.get("asset_id")
            try:
                row_values = dict(row)
                row_values["fiscal_date"] = _parse_date_value(row.get("fiscal_date"))
                for column in NUMERIC_SANITIZE_COLUMNS:
                    sanitized_value, did_sanitize = _sanitize_numeric_value(
                        row_values.get(column)
                    )
                    row_values[column] = sanitized_value
                    if did_sanitize:
                        non_finite_count += 1
                rows.append(tuple(row_values.get(col, 0) for col in ratio_cols))
            except Exception as exc:
                failed_rows.append(
                    {
                        "symbol": str(asset_id or "unknown"),
                        "error": f"ratio row conversion failed: {exc}",
                    }
                )

        if non_finite_count:
            print(
                "load_ratios: sanitized non-finite numeric values "
                f"(count={non_finite_count})"
            )

        print(f"Upserting {len(rows)} financial ratio rows into market_data.financial_ratios...")
        try:
            failed_batches = _upsert_rows_in_batches(
                conn,
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
                table_name="market_data.financial_ratios",
            )
            _report_failed_symbols("load_ratios (row conversion)", failed_rows)
        finally:
            conn.close()
        return {
            "failed_symbols": failed_symbols,
            "failed_rows": failed_rows,
            "failed_batches": failed_batches,
        }

    e_ratio = extract_ratios()
    l_ratio = load_ratios(e_ratio)
    e_ratio >> l_ratio
