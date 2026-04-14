from __future__ import annotations

from typing import TypeAlias

import pandas as pd

from dags.etl_modules.contracts.providers import (
    ExtractPayload,
    FailureRecord,
    FundamentalsFinanceProvider,
    LoadSummary,
    MarketDataWriter,
    Record,
)
from dags.etl_modules.orchestrators.shared_reporting import report_failed_symbols
from dags.etl_modules.transformers.shared_transformers import parse_date_value

FinalizeSummary: TypeAlias = dict[str, int]


def _empty_load_summary() -> LoadSummary:
    return {
        "records_input": 0,
        "rows_prepared": 0,
        "rows_loaded": 0,
        "failed_symbols": [],
        "failed_rows": [],
        "failed_batches": [],
    }


def _records_payload(
    records: list[Record],
    failed_symbols: list[FailureRecord],
) -> ExtractPayload:
    return {"records": records, "failed_symbols": failed_symbols}


def _unpack_records_payload(
    data: ExtractPayload | list[Record] | None,
) -> tuple[list[Record], list[FailureRecord]]:
    if isinstance(data, dict):
        records = data.get("records") or []
        failed_symbols = data.get("failed_symbols") or []
        return list(records), list(failed_symbols)
    if isinstance(data, list):
        return data, []
    return [], []


def _records_from_frames(frames: list[pd.DataFrame]) -> list[Record]:
    if not frames:
        return []
    merged = pd.concat(frames, ignore_index=True)
    if "fiscal_date" in merged.columns:
        merged["fiscal_date"] = merged["fiscal_date"].astype(str)
    return merged.to_dict("records")


def _convert_records_to_rows(
    records: list[Record],
    columns: tuple[str, ...],
    *,
    row_error_prefix: str,
) -> tuple[list[tuple[object, ...]], list[FailureRecord]]:
    rows: list[tuple[object, ...]] = []
    failed_rows: list[FailureRecord] = []
    for row in records:
        asset_id = row.get("asset_id")
        try:
            row_values = dict(row)
            row_values["fiscal_date"] = parse_date_value(row.get("fiscal_date"))
            rows.append(tuple(row_values.get(col) for col in columns))
        except Exception as exc:
            failed_rows.append(
                {
                    "symbol": str(asset_id or "unknown"),
                    "error": f"{row_error_prefix}: {exc}",
                }
            )
    return rows, failed_rows


def extract_income_statements(
    *,
    provider: FundamentalsFinanceProvider,
) -> ExtractPayload:
    assets = provider.list_assets()
    income_frames: list[pd.DataFrame] = []
    failed_symbols: list[FailureRecord] = []

    print(f"Fetching income statements for {len(assets)} tickers...")
    for asset in assets:
        symbol = str(asset.get("symbol") or "").strip().upper()
        asset_id = str(asset.get("asset_id") or "").strip()
        if not symbol or not asset_id:
            failed_symbols.append(
                {
                    "symbol": symbol or "unknown",
                    "error": "missing symbol or asset_id",
                }
            )
            continue
        try:
            frame = provider.fetch_income_statement(symbol, asset_id)
        except Exception as exc:
            failed_symbols.append({"symbol": symbol, "error": str(exc)})
            continue
        if frame is not None and not frame.empty:
            income_frames.append(frame)

    records = _records_from_frames(income_frames)
    report_failed_symbols("extract_income_statements", failed_symbols)
    return _records_payload(records, failed_symbols)


def load_income_statements(
    data: ExtractPayload | list[Record] | None,
    *,
    provider: FundamentalsFinanceProvider,
    repository: MarketDataWriter,
    db_url: str | None,
    batch_size: int,
    upsert_sql: str,
    income_columns: tuple[str, ...],
) -> LoadSummary:
    _ = provider
    records, failed_symbols = _unpack_records_payload(data)
    report_failed_symbols("load_income_statements (extract)", failed_symbols)
    if not records:
        print("No income statement data to load.")
        return {
            "records_input": 0,
            "rows_prepared": 0,
            "rows_loaded": 0,
            "failed_symbols": failed_symbols,
            "failed_rows": [],
            "failed_batches": [],
        }

    rows, failed_rows = _convert_records_to_rows(
        records,
        income_columns,
        row_error_prefix="income-statement row conversion failed",
    )

    failed_batches, fatal_error = repository.upsert_rows(
        db_url=db_url,
        query=upsert_sql,
        rows=rows,
        table_name="market_data.income_statements",
        batch_size=batch_size,
    )
    if fatal_error:
        raise RuntimeError(fatal_error)

    failed_batch_rows = 0
    for item in failed_batches:
        size = item.get("size")
        if isinstance(size, (int, float, str)):
            failed_batch_rows += int(size)
    rows_loaded = max(len(rows) - failed_batch_rows, 0)
    report_failed_symbols("load_income_statements (row conversion)", failed_rows)

    return {
        "records_input": len(records),
        "rows_prepared": len(rows),
        "rows_loaded": rows_loaded,
        "failed_symbols": failed_symbols,
        "failed_rows": failed_rows,
        "failed_batches": failed_batches,
    }


def extract_balance_sheets(
    *,
    provider: FundamentalsFinanceProvider,
) -> ExtractPayload:
    assets = provider.list_assets()
    balance_frames: list[pd.DataFrame] = []
    failed_symbols: list[FailureRecord] = []

    print(f"Fetching balance sheets for {len(assets)} tickers...")
    for asset in assets:
        symbol = str(asset.get("symbol") or "").strip().upper()
        asset_id = str(asset.get("asset_id") or "").strip()
        if not symbol or not asset_id:
            failed_symbols.append(
                {
                    "symbol": symbol or "unknown",
                    "error": "missing symbol or asset_id",
                }
            )
            continue
        try:
            frame = provider.fetch_balance_sheet(symbol, asset_id)
        except Exception as exc:
            failed_symbols.append({"symbol": symbol, "error": str(exc)})
            continue
        if frame is not None and not frame.empty:
            balance_frames.append(frame)

    records = _records_from_frames(balance_frames)
    report_failed_symbols("extract_balance_sheets", failed_symbols)
    return _records_payload(records, failed_symbols)


def load_balance_sheets(
    data: ExtractPayload | list[Record] | None,
    *,
    provider: FundamentalsFinanceProvider,
    repository: MarketDataWriter,
    db_url: str | None,
    batch_size: int,
    upsert_sql: str,
    balance_columns: tuple[str, ...],
) -> LoadSummary:
    _ = provider
    records, failed_symbols = _unpack_records_payload(data)
    report_failed_symbols("load_balance_sheets (extract)", failed_symbols)
    if not records:
        print("No balance sheet data to load.")
        return {
            "records_input": 0,
            "rows_prepared": 0,
            "rows_loaded": 0,
            "failed_symbols": failed_symbols,
            "failed_rows": [],
            "failed_batches": [],
        }

    rows, failed_rows = _convert_records_to_rows(
        records,
        balance_columns,
        row_error_prefix="balance-sheet row conversion failed",
    )

    failed_batches, fatal_error = repository.upsert_rows(
        db_url=db_url,
        query=upsert_sql,
        rows=rows,
        table_name="market_data.balance_sheets",
        batch_size=batch_size,
    )
    if fatal_error:
        raise RuntimeError(fatal_error)

    failed_batch_rows = 0
    for item in failed_batches:
        size = item.get("size")
        if isinstance(size, (int, float, str)):
            failed_batch_rows += int(size)
    rows_loaded = max(len(rows) - failed_batch_rows, 0)
    report_failed_symbols("load_balance_sheets (row conversion)", failed_rows)

    return {
        "records_input": len(records),
        "rows_prepared": len(rows),
        "rows_loaded": rows_loaded,
        "failed_symbols": failed_symbols,
        "failed_rows": failed_rows,
        "failed_batches": failed_batches,
    }


def finalize_fundamentals_load(
    income_summary: LoadSummary | None,
    balance_summary: LoadSummary | None,
) -> FinalizeSummary:
    summaries: dict[str, LoadSummary] = {
        "income_statements": income_summary or _empty_load_summary(),
        "balance_sheets": balance_summary or _empty_load_summary(),
    }

    total_records_input = 0
    total_rows_prepared = 0
    total_rows_loaded = 0
    total_failed_symbols = 0
    total_failed_rows = 0
    total_failed_batches = 0
    failed_batch_tables: list[str] = []
    zero_loaded_tables: list[str] = []

    for table_name, summary in summaries.items():
        records_input = int(summary.get("records_input") or 0)
        rows_prepared = int(summary.get("rows_prepared") or 0)
        rows_loaded = int(summary.get("rows_loaded") or 0)
        failed_symbols = summary.get("failed_symbols") or []
        failed_rows = summary.get("failed_rows") or []
        failed_batches = summary.get("failed_batches") or []

        total_records_input += records_input
        total_rows_prepared += rows_prepared
        total_rows_loaded += rows_loaded
        total_failed_symbols += len(failed_symbols)
        total_failed_rows += len(failed_rows)
        total_failed_batches += len(failed_batches)

        if failed_batches:
            failed_batch_tables.append(table_name)
        if rows_prepared > 0 and rows_loaded == 0:
            zero_loaded_tables.append(table_name)

    print(
        "fundamentals pipeline summary: "
        f"records_input={total_records_input}, "
        f"rows_prepared={total_rows_prepared}, "
        f"rows_loaded={total_rows_loaded}, "
        f"failed_symbols={total_failed_symbols}, "
        f"failed_rows={total_failed_rows}, "
        f"failed_batches={total_failed_batches}"
    )

    if failed_batch_tables or zero_loaded_tables:
        raise RuntimeError(
            "Fundamentals pipeline completed with write failures: "
            f"failed_batch_tables={failed_batch_tables}, "
            f"zero_loaded_tables={zero_loaded_tables}"
        )

    return {
        "records_input": total_records_input,
        "rows_prepared": total_rows_prepared,
        "rows_loaded": total_rows_loaded,
        "failed_symbols": total_failed_symbols,
        "failed_rows": total_failed_rows,
        "failed_batches": total_failed_batches,
    }
