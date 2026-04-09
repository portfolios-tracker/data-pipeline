import pytest

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

    assert (
        market_data_fundamentals_weekly.dag.dag_id == "market_data_fundamentals_weekly"
    )
    assert market_data_fundamentals_weekly.dag.schedule == "0 19 * * 0"
    assert expected_extract_load_tasks.issubset(
        set(market_data_fundamentals_weekly.dag.task_ids)
    )


@pytest.mark.unit
def test_extract_income_statements_delegates_to_orchestrator(monkeypatch):
    expected_payload = {"records": [{"asset_id": "a1"}], "failed_symbols": []}
    captured = {}

    def _fake_extract(**kwargs):
        captured.update(kwargs)
        return expected_payload

    monkeypatch.setattr(
        market_data_fundamentals_weekly.fundamentals_orchestrator,
        "extract_income_statements",
        _fake_extract,
    )

    result = market_data_fundamentals_weekly.extract_income_statements.function()

    assert result == expected_payload
    assert captured["provider"] is market_data_fundamentals_weekly.FUNDAMENTALS_PROVIDER


@pytest.mark.unit
def test_load_income_statements_delegates_to_orchestrator(monkeypatch):
    payload = {"records": [{"asset_id": "a1"}], "failed_symbols": []}
    expected_summary = {
        "records_input": 1,
        "rows_prepared": 1,
        "rows_loaded": 1,
        "failed_symbols": [],
        "failed_rows": [],
        "failed_batches": [],
    }
    captured = {}

    def _fake_load(data, **kwargs):
        captured["data"] = data
        captured.update(kwargs)
        return expected_summary

    monkeypatch.setattr(
        market_data_fundamentals_weekly.fundamentals_orchestrator,
        "load_income_statements",
        _fake_load,
    )

    result = market_data_fundamentals_weekly.load_income_statements.function(payload)

    assert result == expected_summary
    assert captured["data"] == payload
    assert captured["provider"] is market_data_fundamentals_weekly.FUNDAMENTALS_PROVIDER
    assert (
        captured["repository"] is market_data_fundamentals_weekly.MARKET_DATA_REPOSITORY
    )
    assert captured["db_url"] == market_data_fundamentals_weekly.SUPABASE_DB_URL
    assert (
        captured["batch_size"] == market_data_fundamentals_weekly.DB_UPSERT_BATCH_SIZE
    )
    assert (
        captured["upsert_sql"]
        == market_data_fundamentals_weekly.INCOME_STATEMENTS_UPSERT_SQL
    )
    assert (
        captured["income_columns"]
        == market_data_fundamentals_weekly.INCOME_STATEMENT_COLUMNS
    )


@pytest.mark.unit
def test_load_balance_sheets_delegates_to_orchestrator(monkeypatch):
    payload = {"records": [{"asset_id": "a1"}], "failed_symbols": []}
    expected_summary = {
        "records_input": 1,
        "rows_prepared": 1,
        "rows_loaded": 1,
        "failed_symbols": [],
        "failed_rows": [],
        "failed_batches": [],
    }
    captured = {}

    def _fake_load(data, **kwargs):
        captured["data"] = data
        captured.update(kwargs)
        return expected_summary

    monkeypatch.setattr(
        market_data_fundamentals_weekly.fundamentals_orchestrator,
        "load_balance_sheets",
        _fake_load,
    )

    result = market_data_fundamentals_weekly.load_balance_sheets.function(payload)

    assert result == expected_summary
    assert captured["data"] == payload
    assert captured["provider"] is market_data_fundamentals_weekly.FUNDAMENTALS_PROVIDER
    assert (
        captured["repository"] is market_data_fundamentals_weekly.MARKET_DATA_REPOSITORY
    )
    assert captured["db_url"] == market_data_fundamentals_weekly.SUPABASE_DB_URL
    assert (
        captured["batch_size"] == market_data_fundamentals_weekly.DB_UPSERT_BATCH_SIZE
    )
    assert (
        captured["upsert_sql"]
        == market_data_fundamentals_weekly.BALANCE_SHEETS_UPSERT_SQL
    )
    assert (
        captured["balance_columns"]
        == market_data_fundamentals_weekly.BALANCE_SHEET_COLUMNS
    )


@pytest.mark.unit
def test_finalize_fundamentals_load_delegates_to_orchestrator(monkeypatch):
    income_summary = {"rows_loaded": 1}
    balance_summary = {"rows_loaded": 2}
    expected = {
        "records_input": 3,
        "rows_prepared": 3,
        "rows_loaded": 3,
        "failed_symbols": 0,
        "failed_rows": 0,
        "failed_batches": 0,
    }

    def _fake_finalize(income, balance):
        assert income == income_summary
        assert balance == balance_summary
        return expected

    monkeypatch.setattr(
        market_data_fundamentals_weekly.fundamentals_orchestrator,
        "finalize_fundamentals_load",
        _fake_finalize,
    )

    assert (
        market_data_fundamentals_weekly.finalize_fundamentals_load.function(
            income_summary,
            balance_summary,
        )
        == expected
    )
