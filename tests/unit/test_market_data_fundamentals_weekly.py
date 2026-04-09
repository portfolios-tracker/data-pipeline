import sys
import types
from unittest.mock import MagicMock, patch

import pytest

try:
    import psycopg2  # noqa: F401
except ModuleNotFoundError:
    psycopg2_stub = types.ModuleType("psycopg2")
    psycopg2_stub.connect = MagicMock()
    psycopg2_stub.extras = types.SimpleNamespace(execute_values=MagicMock())
    sys.modules["psycopg2"] = psycopg2_stub
    sys.modules["psycopg2.extras"] = psycopg2_stub.extras

from dags import market_data_fundamentals_weekly


@pytest.mark.unit
def test_weekly_fundamentals_dag_has_expected_identity_schedule_and_tasks():
    expected_extract_load_tasks = {
        "fundamental_pipeline.extract_income_statements",
        "fundamental_pipeline.load_income_statements",
        "balance_sheet_group.extract_balance_sheets",
        "balance_sheet_group.load_balance_sheets",
        "finalize_fundamentals_load",
    }

    assert market_data_fundamentals_weekly.dag.dag_id == "market_data_fundamentals_weekly"
    assert market_data_fundamentals_weekly.dag.schedule == "0 19 * * 0"
    assert expected_extract_load_tasks.issubset(
        set(market_data_fundamentals_weekly.dag.task_ids)
    )


@pytest.mark.unit
def test_chunked_rows_splits_into_expected_batch_sizes():
    rows = list(range(205))

    chunks = list(market_data_fundamentals_weekly._chunked_rows(rows, 100))

    assert [len(chunk) for chunk in chunks] == [100, 100, 5]


@pytest.mark.unit
@patch("dags.market_data_fundamentals_weekly.psycopg2.extras.execute_values")
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

    failed_batches = market_data_fundamentals_weekly._upsert_rows_in_batches(
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
def test_unpack_payload_supports_dict_and_list():
    records, failed = market_data_fundamentals_weekly._unpack_payload(
        {"records": [{"asset_id": "a1"}], "failed_symbols": [{"symbol": "AAA"}]}
    )
    assert records == [{"asset_id": "a1"}]
    assert failed == [{"symbol": "AAA"}]

    legacy_records, legacy_failed = market_data_fundamentals_weekly._unpack_payload(
        [{"asset_id": "a2"}]
    )
    assert legacy_records == [{"asset_id": "a2"}]
    assert legacy_failed == []


@pytest.mark.unit
def test_parse_date_value_handles_iso_and_sentinel_values():
    parsed = market_data_fundamentals_weekly._parse_date_value("2026-04-06")
    assert parsed.isoformat() == "2026-04-06"
    assert market_data_fundamentals_weekly._parse_date_value("NaT") is None


@pytest.mark.unit
def test_finalize_fundamentals_load_raises_when_failed_batches_exist():
    finalizer = getattr(
        market_data_fundamentals_weekly,
        "finalize_fundamentals_load",
        None,
    )
    assert finalizer is not None

    income_summary = {
        "records_input": 8,
        "rows_prepared": 8,
        "rows_loaded": 6,
        "failed_symbols": [],
        "failed_rows": [],
        "failed_batches": [{"batch_index": 1, "size": 2, "error": "db timeout"}],
    }
    balance_summary = {
        "records_input": 8,
        "rows_prepared": 8,
        "rows_loaded": 8,
        "failed_symbols": [],
        "failed_rows": [],
        "failed_batches": [],
    }

    with pytest.raises(RuntimeError):
        finalizer.function(income_summary, balance_summary)
