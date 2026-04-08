from unittest.mock import MagicMock, patch

import pytest

from dags import market_data_events_daily


@pytest.mark.unit
def test_events_daily_dag_has_expected_identity_schedule_and_tasks():
    assert market_data_events_daily.dag.dag_id == "market_data_events_daily"
    assert market_data_events_daily.dag.schedule == "20 18 * * 1-5"
    assert sorted(task.task_id for task in market_data_events_daily.dag.tasks) == [
        "extract_corporate_events",
        "load_corporate_events",
    ]


@pytest.mark.unit
def test_chunked_rows_splits_into_expected_batch_sizes():
    rows = list(range(205))

    chunks = list(market_data_events_daily._chunked_rows(rows, 100))

    assert [len(chunk) for chunk in chunks] == [100, 100, 5]


@pytest.mark.unit
@patch("dags.market_data_events_daily.psycopg2.extras.execute_values")
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

    failed_batches = market_data_events_daily._upsert_rows_in_batches(
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
    records, failed = market_data_events_daily._unpack_payload(
        {"records": [{"asset_id": "a1"}], "failed_symbols": [{"symbol": "AAA"}]}
    )
    assert records == [{"asset_id": "a1"}]
    assert failed == [{"symbol": "AAA"}]

    legacy_records, legacy_failed = market_data_events_daily._unpack_payload(
        [{"asset_id": "a2"}]
    )
    assert legacy_records == [{"asset_id": "a2"}]
    assert legacy_failed == []


@pytest.mark.unit
def test_parse_date_value_handles_iso_and_sentinel_values():
    parsed = market_data_events_daily._parse_date_value("2026-04-06")
    assert parsed.isoformat() == "2026-04-06"
    assert market_data_events_daily._parse_date_value("NaT") is None
