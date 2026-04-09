import math
from typing import Any

import pandas as pd

from dags.etl_modules.transformers.shared_transformers import parse_date_value


def sanitize_numeric_value(value: Any) -> tuple[Any | None, bool]:
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


def records_from_frames(ratio_frames: list[pd.DataFrame]) -> list[dict[str, Any]]:
    valid_frames = [
        frame for frame in ratio_frames if frame is not None and not frame.empty
    ]
    if not valid_frames:
        return []
    return pd.concat(valid_frames, ignore_index=True).to_dict("records")


def convert_records_to_rows(
    records: list[dict[str, Any]],
    ratio_columns: tuple[str, ...],
    numeric_sanitize_columns: tuple[str, ...],
) -> tuple[list[tuple[Any, ...]], list[dict[str, str]], int]:
    rows: list[tuple[Any, ...]] = []
    failed_rows: list[dict[str, str]] = []
    non_finite_count = 0

    for row in records:
        asset_id = row.get("asset_id")
        symbol = row.get("symbol")
        try:
            row_values = dict(row)
            row_values["fiscal_date"] = parse_date_value(row.get("fiscal_date"))
            for column in numeric_sanitize_columns:
                sanitized_value, did_sanitize = sanitize_numeric_value(
                    row_values.get(column)
                )
                row_values[column] = sanitized_value
                if did_sanitize:
                    non_finite_count += 1
            rows.append(tuple(row_values.get(col) for col in ratio_columns))
        except Exception as exc:
            failed_rows.append(
                {
                    "symbol": str(symbol or "unknown"),
                    "asset_id": str(asset_id or "unknown"),
                    "error": f"ratio row conversion failed: {exc}",
                }
            )

    return rows, failed_rows, non_finite_count
