from unittest.mock import MagicMock, patch

import pytest
from dags import market_data_prices_daily


@pytest.mark.unit
def test_prices_daily_dag_has_expected_identity_schedule_and_tasks():
    assert market_data_prices_daily.dag.dag_id == "market_data_prices_daily"
    assert market_data_prices_daily.dag.schedule == "0 18 * * 1-5"
    assert sorted(task.task_id for task in market_data_prices_daily.dag.tasks) == [
        "chunk_price_assets",
        "finalize_prices_load",
        "list_price_assets",
        "process_price_chunk",
    ]


@pytest.mark.unit
def test_chunked_rows_splits_into_expected_batch_sizes():
    rows = list(range(205))

    chunks = list(market_data_prices_daily._chunked_rows(rows, 100))

    assert [len(chunk) for chunk in chunks] == [100, 100, 5]


@pytest.mark.unit
@patch("dags.market_data_prices_daily.psycopg2.extras.execute_values")
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

    failed_batches = market_data_prices_daily._upsert_rows_in_batches(
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
def test_chunk_price_assets_splits_assets_by_batch_size(monkeypatch):
    monkeypatch.setattr(market_data_prices_daily, "DB_UPSERT_BATCH_SIZE", 2)
    assets = [
        {"symbol": "AAA", "asset_id": "1"},
        {"symbol": "BBB", "asset_id": "2"},
        {"symbol": "CCC", "asset_id": "3"},
        {"symbol": "DDD", "asset_id": "4"},
        {"symbol": "EEE", "asset_id": "5"},
    ]

    chunks = market_data_prices_daily.chunk_price_assets.function(assets)

    assert len(chunks) == 3
    assert chunks[0]["chunk_index"] == 1
    assert len(chunks[0]["assets"]) == 2
    assert chunks[2]["chunk_index"] == 3
    assert len(chunks[2]["assets"]) == 1


@pytest.mark.unit
def test_parse_date_value_handles_iso_and_sentinel_values():
    parsed = market_data_prices_daily._parse_date_value("2026-04-06")
    assert parsed is not None
    assert parsed.isoformat() == "2026-04-06"
    assert market_data_prices_daily._parse_date_value("NaT") is None


@pytest.mark.unit
def test_finalize_prices_load_returns_alert_after_partial_failures():
    chunk_results = [
        {
            "chunk_index": 1,
            "chunk_assets": 2,
            "records_extracted": 10,
            "rows_prepared": 10,
            "rows_loaded": 10,
            "failed_symbols": [],
            "failed_rows": [],
            "failed_batches": [],
            "fatal_error": None,
        },
        {
            "chunk_index": 2,
            "chunk_assets": 2,
            "records_extracted": 8,
            "rows_prepared": 8,
            "rows_loaded": 7,
            "failed_symbols": [{"symbol": "BBB", "error": "timeout"}],
            "failed_rows": [],
            "failed_batches": [{"batch_index": 1, "size": 1, "error": "db"}],
            "fatal_error": None,
        },
    ]

    result = market_data_prices_daily.finalize_prices_load.function(chunk_results)

    assert result["alert_mode"] is True
    assert result["failed_symbols"] == 1
    assert result["failed_rows"] == 0
    assert result["failed_batches"] == 1
    assert result["rows_loaded"] == 17
