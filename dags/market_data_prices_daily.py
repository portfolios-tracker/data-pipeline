from airflow import DAG
from airflow.sdk import task
from datetime import datetime, timedelta
from itertools import islice
from pendulum import timezone
import pandas as pd
import psycopg2
import psycopg2.extras
import os

try:
    from etl_modules.fetcher import fetch_stock_price, get_active_vn_stock_tickers
    from etl_modules.notifications import (
        send_success_notification,
        send_failure_notification,
    )
except ModuleNotFoundError as exc:
    if exc.name != "etl_modules":
        raise
    from dags.etl_modules.fetcher import fetch_stock_price, get_active_vn_stock_tickers
    from dags.etl_modules.notifications import (
        send_success_notification,
        send_failure_notification,
    )


# CONFIG
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")
DB_UPSERT_BATCH_SIZE = int(os.getenv("DB_UPSERT_BATCH_SIZE", "100"))

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
    dag_id="market_data_prices_daily",
    default_args=default_args,
    schedule="0 18 * * 1-5",  # 6 PM Vietnam Time Mon-Fri
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),
    catchup=False,
    tags=["stock-price", "supabase", "evening-batch"],
    on_success_callback=send_success_notification,
    on_failure_callback=send_failure_notification,
) as dag:
    @task
    def extract_prices():
        price_data = []
        failed_symbols = []
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
            try:
                df_price = fetch_stock_price(symbol, asset_id, lookback_date, end_date)
            except Exception as exc:
                failed_symbols.append({"symbol": symbol, "error": str(exc)})
                print(f"Price extraction failed for {symbol}: {exc}")
                continue
            if not df_price.empty:
                # Filter to only last 7 days for insertion
                df_price = df_price[df_price["trading_date"].astype(str) >= filter_from]
                price_data.append(df_price)

        if price_data:
            final_price_df = pd.concat(price_data, ignore_index=True)
            # Convert date objects to strings for JSON serialization
            if "trading_date" in final_price_df.columns:
                final_price_df["trading_date"] = final_price_df["trading_date"].astype(str)
            records = final_price_df.to_dict("records")
        else:
            records = []
        _report_failed_symbols("extract_prices", failed_symbols)
        return _to_payload(records, failed_symbols)

    @task
    def load_prices(data):
        records, failed_symbols = _unpack_payload(data)
        _report_failed_symbols("load_prices (extract)", failed_symbols)
        if not records:
            print("No price data to load.")
            return {"failed_symbols": failed_symbols, "failed_rows": [], "failed_batches": []}

        if not SUPABASE_DB_URL:
            raise RuntimeError("SUPABASE_DB_URL environment variable is not set")

        print("Connecting to Supabase/Postgres...")
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
        failed_rows = []
        for row in records:
            asset_id = row.get("asset_id")
            try:
                parsed_date = _parse_date_value(row.get("trading_date"))
                row_values = dict(row)
                row_values["trading_date"] = parsed_date
                rows.append(tuple(row_values.get(col) for col in price_cols))
            except Exception as exc:
                failed_rows.append(
                    {
                        "symbol": str(asset_id or "unknown"),
                        "error": f"price row conversion failed: {exc}",
                    }
                )

        print(f"Upserting {len(rows)} price rows into market_data.prices...")
        try:
            failed_batches = _upsert_rows_in_batches(
                conn,
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
                table_name="market_data.prices",
            )
            _report_failed_symbols("load_prices (row conversion)", failed_rows)
        finally:
            conn.close()
        return {
            "failed_symbols": failed_symbols,
            "failed_rows": failed_rows,
            "failed_batches": failed_batches,
        }

    extracted_prices = extract_prices()
    loaded_prices = load_prices(extracted_prices)
    extracted_prices >> loaded_prices
