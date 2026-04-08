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
    from etl_modules.fetcher import (
        fetch_corporate_events,
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
        fetch_corporate_events,
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
    dag_id="market_data_events_daily",
    default_args=default_args,
    schedule="20 18 * * 1-5",  # 6:20 PM Vietnam Time Mon-Fri
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),
    catchup=False,
    tags=["corporate-events", "supabase", "evening-batch"],
    on_success_callback=send_success_notification,
    on_failure_callback=send_failure_notification,
) as dag:
    @task
    def extract_corporate_events():
        events_data = []
        failed_symbols = []
        assets = get_active_vn_stock_tickers(raise_on_fallback=True)

        print(f"Fetching corporate events for {len(assets)} tickers...")
        for asset in assets:
            symbol = asset["symbol"]
            try:
                df_ev = fetch_corporate_events(symbol, asset["asset_id"])
            except Exception as exc:
                failed_symbols.append({"symbol": symbol, "error": str(exc)})
                print(f"Corporate events extraction failed for {symbol}: {exc}")
                continue
            if not df_ev.empty:
                events_data.append(df_ev)

        if events_data:
            df_ev_final = pd.concat(events_data, ignore_index=True)
            for dcol in ["event_date", "public_date", "exright_date"]:
                if dcol in df_ev_final.columns:
                    df_ev_final[dcol] = df_ev_final[dcol].astype(str)
            records = df_ev_final.to_dict("records")
        else:
            records = []
        _report_failed_symbols("extract_corporate_events", failed_symbols)
        return _to_payload(records, failed_symbols)

    @task
    def load_corporate_events(data):
        records, failed_symbols = _unpack_payload(data)
        _report_failed_symbols("load_corporate_events (extract)", failed_symbols)
        if not records:
            print("No corporate event data to load.")
            return {"failed_symbols": failed_symbols, "failed_rows": [], "failed_batches": []}

        if not SUPABASE_DB_URL:
            raise RuntimeError("SUPABASE_DB_URL environment variable is not set")

        conn = psycopg2.connect(SUPABASE_DB_URL)

        try:
            print(
                f"Upserting {len(records)} corporate event rows into market_data.corporate_events..."
            )
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
            failed_rows = []
            for row in records:
                asset_id = row.get("asset_id")
                try:
                    row_values = dict(row)
                    for dcol in ["event_date", "public_date", "exright_date"]:
                        row_values[dcol] = _parse_date_value(row.get(dcol))
                    ev_rows.append(tuple(row_values.get(col) for col in ev_cols))
                except Exception as exc:
                    failed_rows.append(
                        {
                            "symbol": str(asset_id or "unknown"),
                            "error": f"corporate-events row conversion failed: {exc}",
                        }
                    )

            failed_batches = _upsert_rows_in_batches(
                conn,
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
                table_name="market_data.corporate_events",
            )
            _report_failed_symbols("load_corporate_events (row conversion)", failed_rows)
        finally:
            conn.close()
        return {
            "failed_symbols": failed_symbols,
            "failed_rows": failed_rows,
            "failed_batches": failed_batches,
        }

    e_ev = extract_corporate_events()
    l_ev = load_corporate_events(e_ev)
    e_ev >> l_ev
