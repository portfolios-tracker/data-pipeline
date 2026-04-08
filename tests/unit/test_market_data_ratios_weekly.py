from unittest.mock import MagicMock, patch

import pytest

from dags import market_data_ratios_weekly


@pytest.mark.unit
def test_weekly_ratios_dag_has_expected_identity_schedule_and_tasks():
    assert market_data_ratios_weekly.dag.dag_id == "market_data_ratios_weekly"
    assert market_data_ratios_weekly.dag.schedule == "0 19 * * 0"
    assert sorted(task.task_id for task in market_data_ratios_weekly.dag.tasks) == [
        "extract_ratios",
        "load_ratios",
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
def test_unpack_payload_supports_dict_and_list():
    records, failed = market_data_ratios_weekly._unpack_payload(
        {"records": [{"asset_id": "a1"}], "failed_symbols": [{"symbol": "AAA"}]}
    )
    assert records == [{"asset_id": "a1"}]
    assert failed == [{"symbol": "AAA"}]

    legacy_records, legacy_failed = market_data_ratios_weekly._unpack_payload(
        [{"asset_id": "a2"}]
    )
    assert legacy_records == [{"asset_id": "a2"}]
    assert legacy_failed == []


@pytest.mark.unit
def test_parse_date_value_handles_iso_and_sentinel_values():
    parsed = market_data_ratios_weekly._parse_date_value("2026-04-06")
    assert parsed.isoformat() == "2026-04-06"
    assert market_data_ratios_weekly._parse_date_value("NaT") is None


@pytest.mark.unit
def test_sanitize_numeric_preserves_large_finite_values_and_drops_non_finite():
    finite, finite_flag = market_data_ratios_weekly._sanitize_numeric_value(100000.0)
    infinite, infinite_flag = market_data_ratios_weekly._sanitize_numeric_value(float("inf"))
    nan_value, nan_flag = market_data_ratios_weekly._sanitize_numeric_value(float("nan"))

    assert finite == pytest.approx(100000.0)
    assert finite_flag is False
    assert infinite is None
    assert infinite_flag is True
    assert nan_value is None
    assert nan_flag is False


@pytest.mark.unit
@patch("dags.market_data_ratios_weekly.psycopg2.connect")
@patch("dags.market_data_ratios_weekly.psycopg2.extras.execute_values")
def test_load_ratios_preserves_large_finite_values_before_upsert(
    mock_execute_values, mock_connect, monkeypatch
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

    payload = {
        "records": [
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
        ],
        "failed_symbols": [],
    }

    result = market_data_ratios_weekly.load_ratios.function(payload)
    row = captured["rows"][0]

    assert result["failed_batches"] == []
    assert row[20] == pytest.approx(100000.0)  # interest_coverage is preserved
    assert row[22] == pytest.approx(-12000.0)  # inventory_turnover is preserved
    assert row[18] == pytest.approx(1.5)  # current_ratio remains unchanged
    conn.close.assert_called_once()
