import os
from datetime import datetime, timedelta
from itertools import islice

import pandas as pd
import psycopg2
import psycopg2.extras
from airflow import DAG
from airflow.sdk import task
from pendulum import timezone

try:
    from etl_modules.fetcher import fetch_stock_price, get_active_vn_stock_tickers
    from etl_modules.notifications import (
        send_failure_notification,
        send_success_notification,
    )
except ModuleNotFoundError as exc:
    if exc.name != "etl_modules":
        raise
    from dags.etl_modules.fetcher import fetch_stock_price, get_active_vn_stock_tickers
    from dags.etl_modules.notifications import (
        send_failure_notification,
        send_success_notification,
    )


# CONFIG
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")
DB_UPSERT_BATCH_SIZE = int(os.getenv("DB_UPSERT_BATCH_SIZE", "100"))
VCI_GRAPHQL_POOL = "vci_graphql"
PRICE_INDICATOR_LOOKBACK_DAYS = int(os.getenv("PRICE_INDICATOR_LOOKBACK_DAYS", "250"))
PRICE_LOAD_WINDOW_DAYS = int(os.getenv("PRICE_LOAD_WINDOW_DAYS", "7"))
PRICE_COLUMNS = (
    "trading_date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "asset_id",
    "source",
)
PRICES_UPSERT_SQL = """
INSERT INTO market_data.prices
    (trading_date, open, high, low, close, volume, asset_id, source)
VALUES %s
ON CONFLICT (asset_id, trading_date) DO UPDATE SET
    open          = EXCLUDED.open,
    high          = EXCLUDED.high,
    low           = EXCLUDED.low,
    close         = EXCLUDED.close,
    volume        = EXCLUDED.volume,
    source        = EXCLUDED.source,
    ingested_at   = NOW()
"""

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

    @task(show_return_value_in_logs=False)
    def list_price_assets():
        assets = get_active_vn_stock_tickers(raise_on_fallback=True)
        print(f"Fetched {len(assets)} active VN stock tickers for prices pipeline.")
        return assets

    @task(show_return_value_in_logs=False)
    def chunk_price_assets(assets):
        chunks = []
        for chunk_index, asset_chunk in enumerate(
            _chunked_rows(assets, DB_UPSERT_BATCH_SIZE), start=1
        ):
            chunks.append({"chunk_index": chunk_index, "assets": asset_chunk})

        print(
            f"Prepared {len(chunks)} price chunks (chunk_size={DB_UPSERT_BATCH_SIZE})."
        )
        return chunks

    @task
    def process_price_chunk(chunk_payload):
        chunk_index = int(chunk_payload.get("chunk_index") or 0)
        assets = chunk_payload.get("assets") or []
        price_data = []
        failed_symbols = []

        lookback_date = (
            datetime.today() - timedelta(days=PRICE_INDICATOR_LOOKBACK_DAYS)
        ).strftime("%Y-%m-%d")
        end_date = datetime.today().strftime("%Y-%m-%d")
        filter_from = (
            datetime.today() - timedelta(days=PRICE_LOAD_WINDOW_DAYS)
        ).strftime("%Y-%m-%d")
        print(
            f"process_price_chunk[{chunk_index}] fetching prices from {lookback_date} "
            f"to {end_date} (filtering to last {PRICE_LOAD_WINDOW_DAYS} days)"
        )

        for asset in assets:
            symbol = str(asset.get("symbol") or "").strip().upper()
            asset_id = str(asset.get("asset_id") or "").strip()
            if not symbol or not asset_id:
                failed_symbols.append(
                    {
                        "symbol": symbol or "unknown",
                        "error": "missing symbol or asset_id in chunk payload",
                    }
                )
                continue
            try:
                df_price = fetch_stock_price(symbol, asset_id, lookback_date, end_date)
            except Exception as exc:
                failed_symbols.append({"symbol": symbol, "error": str(exc)})
                continue
            if not df_price.empty:
                df_price = df_price[df_price["trading_date"].astype(str) >= filter_from]
                price_data.append(df_price)

        records = []
        if price_data:
            final_price_df = pd.concat(price_data, ignore_index=True)
            records = final_price_df.to_dict("records")

        rows = []
        failed_rows = []
        for row in records:
            asset_id = row.get("asset_id")
            try:
                parsed_date = _parse_date_value(row.get("trading_date"))
                row_values = dict(row)
                row_values["trading_date"] = parsed_date
                rows.append(tuple(row_values.get(col) for col in PRICE_COLUMNS))
            except Exception as exc:
                failed_rows.append(
                    {
                        "symbol": str(asset_id or "unknown"),
                        "error": f"price row conversion failed: {exc}",
                    }
                )

        failed_batches = []
        fatal_error = None
        if rows and not SUPABASE_DB_URL:
            fatal_error = "SUPABASE_DB_URL environment variable is not set"

        if rows and SUPABASE_DB_URL:
            print(
                f"Upserting {len(rows)} price rows for chunk {chunk_index} into "
                "market_data.prices..."
            )
            conn = None
            try:
                conn = psycopg2.connect(SUPABASE_DB_URL)
                failed_batches = _upsert_rows_in_batches(
                    conn,
                    PRICES_UPSERT_SQL,
                    rows,
                    table_name="market_data.prices",
                )
            except Exception as exc:
                fatal_error = str(exc)
            finally:
                if conn:
                    conn.close()

        _report_failed_symbols(f"process_price_chunk[{chunk_index}]", failed_symbols)
        _report_failed_symbols(
            f"process_price_chunk[{chunk_index}] (row conversion)", failed_rows
        )
        failed_batch_rows = sum(int(item.get("size") or 0) for item in failed_batches)
        loaded_rows = max(len(rows) - failed_batch_rows, 0)

        summary = {
            "chunk_index": chunk_index,
            "chunk_assets": len(assets),
            "records_extracted": len(records),
            "rows_prepared": len(rows),
            "rows_loaded": loaded_rows,
            "failed_symbols": failed_symbols,
            "failed_rows": failed_rows,
            "failed_batches": failed_batches,
            "fatal_error": fatal_error,
        }
        print(
            "process_price_chunk summary: "
            f"chunk={chunk_index}, assets={summary['chunk_assets']}, "
            f"extracted={summary['records_extracted']}, "
            f"loaded={summary['rows_loaded']}, "
            f"failed_symbols={len(failed_symbols)}, "
            f"failed_rows={len(failed_rows)}, "
            f"failed_batches={len(failed_batches)}, "
            f"fatal_error={bool(fatal_error)}"
        )
        return summary

    @task(trigger_rule="all_done")
    def finalize_prices_load(chunk_results):
        results = [
            result for result in (chunk_results or []) if isinstance(result, dict)
        ]
        if not results:
            raise RuntimeError("No chunk results were produced for prices pipeline")

        total_assets = sum(int(result.get("chunk_assets") or 0) for result in results)
        total_extracted = sum(
            int(result.get("records_extracted") or 0) for result in results
        )
        total_loaded = sum(int(result.get("rows_loaded") or 0) for result in results)

        failed_symbols = []
        failed_rows = []
        failed_batches = []
        fatal_errors = []
        for result in results:
            failed_symbols.extend(result.get("failed_symbols") or [])
            failed_rows.extend(result.get("failed_rows") or [])
            failed_batches.extend(result.get("failed_batches") or [])
            fatal_error = result.get("fatal_error")
            if fatal_error:
                fatal_errors.append(
                    {
                        "symbol": f"chunk-{result.get('chunk_index')}",
                        "error": str(fatal_error),
                    }
                )

        print(
            "prices pipeline summary: "
            f"chunks={len(results)}, assets={total_assets}, extracted={total_extracted}, "
            f"loaded={total_loaded}, failed_symbols={len(failed_symbols)}, "
            f"failed_rows={len(failed_rows)}, failed_batches={len(failed_batches)}, "
            f"fatal_errors={len(fatal_errors)}"
        )

        _report_failed_symbols("finalize_prices_load (symbol failures)", failed_symbols)
        _report_failed_symbols("finalize_prices_load (row failures)", failed_rows)
        _report_failed_symbols("finalize_prices_load (fatal errors)", fatal_errors)

        if fatal_errors:
            raise RuntimeError(
                "Prices pipeline completed with fatal chunk errors: "
                f"fatal_errors={len(fatal_errors)}"
            )

        alert_mode = bool(failed_symbols or failed_rows or failed_batches)
        if alert_mode:
            print(
                "prices pipeline alert mode: partial failures detected but DAG will "
                "complete successfully to avoid replaying already-loaded chunks. "
                f"failed_symbols={len(failed_symbols)}, "
                f"failed_rows={len(failed_rows)}, "
                f"failed_batches={len(failed_batches)}"
            )

        return {
            "chunks": len(results),
            "assets": total_assets,
            "records_extracted": total_extracted,
            "rows_loaded": total_loaded,
            "alert_mode": alert_mode,
            "failed_symbols": len(failed_symbols),
            "failed_rows": len(failed_rows),
            "failed_batches": len(failed_batches),
        }

    price_assets = list_price_assets()
    price_chunks = chunk_price_assets(price_assets)
    chunk_summaries = process_price_chunk.override(pool=VCI_GRAPHQL_POOL).expand(
        chunk_payload=price_chunks
    )
    final_summary = finalize_prices_load(chunk_summaries)

    _ = price_assets >> price_chunks >> chunk_summaries >> final_summary
