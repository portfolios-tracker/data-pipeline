import sys
import types
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

try:
    import psycopg2  # noqa: F401
except ModuleNotFoundError:
    psycopg2_stub = types.ModuleType("psycopg2")
    psycopg2_stub.connect = MagicMock()
    psycopg2_stub.extras = types.SimpleNamespace(execute_values=MagicMock())
    sys.modules["psycopg2"] = psycopg2_stub
    sys.modules["psycopg2.extras"] = psycopg2_stub.extras

from dags import market_data_ratios_weekly


@pytest.mark.unit
def test_weekly_ratios_dag_has_expected_identity_schedule_and_tasks():
    assert market_data_ratios_weekly.dag.dag_id == "market_data_ratios_weekly"
    assert market_data_ratios_weekly.dag.schedule == "0 19 * * 0"
    assert sorted(task.task_id for task in market_data_ratios_weekly.dag.tasks) == [
        "chunk_ratio_assets",
        "finalize_ratio_load",
        "list_ratio_assets",
        "process_ratio_chunk",
    ]


@pytest.mark.unit
def test_chunked_rows_splits_into_expected_batch_sizes():
    rows = list(range(205))

    chunks = list(market_data_ratios_weekly._chunked_rows(rows, 100))

    assert [len(chunk) for chunk in chunks] == [100, 100, 5]


@pytest.mark.unit
@patch("dags.market_data_ratios_weekly.psycopg2.extras.execute_values")
def test_upsert_rows_in_batches_continues_on_failed_batch(mock_execute_values):
    conn = MagicMock()
    conn.__enter__.return_value = conn
    conn.__exit__.return_value = False

    cursor = MagicMock()
    cursor.__enter__.return_value = cursor
    cursor.__exit__.return_value = False
    conn.cursor.return_value = cursor

    call_count = {"value": 0}

    def _execute(_cur, _query, batch_rows):
        call_count["value"] += 1
        if call_count["value"] == 2:
            raise RuntimeError("temporary DB failure")
        return batch_rows

    mock_execute_values.side_effect = _execute
    rows = [(i,) for i in range(250)]

    failed_batches = market_data_ratios_weekly._upsert_rows_in_batches(
        conn,
        "INSERT INTO any_table VALUES %s",
        rows,
        table_name="any_table",
    )

    assert call_count["value"] == 3
    assert len(failed_batches) == 1
    assert failed_batches[0]["batch_index"] == 2
    assert conn.rollback.call_count == 1


@pytest.mark.unit
def test_chunk_ratio_assets_splits_assets_by_batch_size(monkeypatch):
    monkeypatch.setattr(market_data_ratios_weekly, "DB_UPSERT_BATCH_SIZE", 2)
    assets = [
        {"symbol": "AAA", "asset_id": "1"},
        {"symbol": "BBB", "asset_id": "2"},
        {"symbol": "CCC", "asset_id": "3"},
        {"symbol": "DDD", "asset_id": "4"},
        {"symbol": "EEE", "asset_id": "5"},
    ]

    chunks = market_data_ratios_weekly.chunk_ratio_assets.function(assets)

    assert len(chunks) == 3
    assert chunks[0]["chunk_index"] == 1
    assert len(chunks[0]["assets"]) == 2
    assert chunks[2]["chunk_index"] == 3
    assert len(chunks[2]["assets"]) == 1


@pytest.mark.unit
def test_parse_date_value_handles_iso_and_sentinel_values():
    parsed = market_data_ratios_weekly._parse_date_value("2026-04-06")
    assert parsed.isoformat() == "2026-04-06"
    assert market_data_ratios_weekly._parse_date_value("NaT") is None


@pytest.mark.unit
def test_sanitize_numeric_preserves_large_finite_values_and_drops_non_finite():
    finite, finite_flag = market_data_ratios_weekly._sanitize_numeric_value(100000.0)
    infinite, infinite_flag = market_data_ratios_weekly._sanitize_numeric_value(
        float("inf")
    )
    nan_value, nan_flag = market_data_ratios_weekly._sanitize_numeric_value(
        float("nan")
    )

    assert finite == pytest.approx(100000.0)
    assert finite_flag is False
    assert infinite is None
    assert infinite_flag is True
    assert nan_value is None
    assert nan_flag is False


@pytest.mark.unit
@patch("dags.market_data_ratios_weekly.psycopg2.connect")
@patch("dags.market_data_ratios_weekly.psycopg2.extras.execute_values")
@patch("dags.market_data_ratios_weekly.fetch_financial_ratios")
def test_process_ratio_chunk_preserves_large_finite_values_before_upsert(
    mock_fetch_financial_ratios, mock_execute_values, mock_connect, monkeypatch
):
    monkeypatch.setattr(
        market_data_ratios_weekly, "SUPABASE_DB_URL", "postgresql://example.local/test"
    )

    conn = MagicMock()
    conn.__enter__.return_value = conn
    conn.__exit__.return_value = False
    cursor = MagicMock()
    cursor.__enter__.return_value = cursor
    cursor.__exit__.return_value = False
    conn.cursor.return_value = cursor
    mock_connect.return_value = conn

    captured = {}

    def _capture(_cur, _query, batch_rows):
        captured["rows"] = batch_rows

    mock_execute_values.side_effect = _capture

    mock_fetch_financial_ratios.return_value = pd.DataFrame(
        [
            {
                "asset_id": "1816833c-6a63-4441-ab32-8a65660c23da",
                "ticker": "BMP",
                "fiscal_date": "2025-12-31",
                "year": 2025,
                "quarter": 4,
                "pe_ratio": 8.0,
                "pb_ratio": 1.2,
                "ps_ratio": 1.5,
                "p_cashflow_ratio": 3.1,
                "eps": 100.0,
                "bvps": 200.0,
                "market_cap": 3000000000.0,
                "roe": 0.1,
                "roa": 0.02,
                "roic": 0.03,
                "financial_leverage": 2.0,
                "dividend_yield": 0.0,
                "net_profit_margin": 0.04,
                "debt_to_equity": 1.1,
                "current_ratio": 1.5,
                "quick_ratio": 1.1,
                "interest_coverage": 100000.0,
                "asset_turnover": 0.9,
                "inventory_turnover": -12000.0,
                "receivable_turnover": 0.0,
                "revenue_growth": 0.01,
                "profit_growth": 0.02,
                "operating_margin": 0.03,
                "gross_margin": 0.04,
                "free_cash_flow": 12345.67,
            }
        ]
    )

    result = market_data_ratios_weekly.process_ratio_chunk.function(
        {
            "chunk_index": 1,
            "assets": [
                {
                    "symbol": "BMP",
                    "asset_id": "1816833c-6a63-4441-ab32-8a65660c23da",
                }
            ],
        }
    )
    row = captured["rows"][0]
    interest_coverage_idx = market_data_ratios_weekly.RATIO_COLUMNS.index(
        "interest_coverage"
    )
    inventory_turnover_idx = market_data_ratios_weekly.RATIO_COLUMNS.index(
        "inventory_turnover"
    )
    current_ratio_idx = market_data_ratios_weekly.RATIO_COLUMNS.index("current_ratio")

    assert result["failed_batches"] == []
    assert result["rows_loaded"] == 1
    assert row[interest_coverage_idx] == pytest.approx(
        100000.0
    )  # interest_coverage is preserved
    assert row[inventory_turnover_idx] == pytest.approx(
        -12000.0
    )  # inventory_turnover is preserved
    assert row[current_ratio_idx] == pytest.approx(1.5)  # current_ratio unchanged
    conn.close.assert_called_once()


@pytest.mark.unit
def test_finalize_ratio_load_raises_on_failed_batches():
    chunk_results = [
        {
            "chunk_index": 1,
            "chunk_assets": 2,
            "records_extracted": 4,
            "rows_prepared": 4,
            "rows_loaded": 4,
            "failed_symbols": [],
            "failed_rows": [],
            "failed_batches": [],
            "fatal_error": None,
        },
        {
            "chunk_index": 2,
            "chunk_assets": 2,
            "records_extracted": 3,
            "rows_prepared": 3,
            "rows_loaded": 2,
            "failed_symbols": [{"symbol": "BBB", "error": "timeout"}],
            "failed_rows": [],
            "failed_batches": [{"batch_index": 1, "size": 1, "error": "db"}],
            "fatal_error": None,
        },
    ]

    with pytest.raises(RuntimeError):
        market_data_ratios_weekly.finalize_ratio_load.function(chunk_results)
