import pytest

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
def test_chunk_ratio_assets_delegates_to_orchestrator(monkeypatch):
    assets = [{"symbol": "AAA", "asset_id": "1"}]
    captured = {}

    def _fake_chunk_assets(value, *, chunk_size):
        captured["value"] = value
        captured["chunk_size"] = chunk_size
        return [{"chunk_index": 1, "assets": value}]

    monkeypatch.setattr(
        market_data_ratios_weekly.ratios_orchestrator,
        "chunk_assets",
        _fake_chunk_assets,
    )

    result = market_data_ratios_weekly.chunk_ratio_assets.function(assets)

    assert result == [{"chunk_index": 1, "assets": assets}]
    assert captured["value"] == assets
    assert captured["chunk_size"] == market_data_ratios_weekly.DB_UPSERT_BATCH_SIZE


@pytest.mark.unit
def test_process_ratio_chunk_delegates_to_orchestrator(monkeypatch):
    payload = {"chunk_index": 1, "assets": [{"symbol": "AAA", "asset_id": "1"}]}

    expected_summary = {
        "chunk_index": 1,
        "chunk_assets": 1,
        "records_extracted": 0,
        "rows_prepared": 0,
        "rows_loaded": 0,
        "failed_symbols": [],
        "failed_rows": [],
        "failed_batches": [],
        "fatal_error": None,
    }

    def _fake_process(chunk_payload, **kwargs):
        assert chunk_payload == payload
        assert kwargs["db_url"] == market_data_ratios_weekly.SUPABASE_DB_URL
        assert kwargs["batch_size"] == market_data_ratios_weekly.DB_UPSERT_BATCH_SIZE
        assert kwargs["upsert_sql"] == market_data_ratios_weekly.RATIOS_UPSERT_SQL
        assert kwargs["ratio_columns"] == market_data_ratios_weekly.RATIO_COLUMNS
        assert (
            kwargs["numeric_sanitize_columns"]
            == market_data_ratios_weekly.NUMERIC_SANITIZE_COLUMNS
        )
        return expected_summary

    monkeypatch.setattr(
        market_data_ratios_weekly.ratios_orchestrator,
        "process_ratio_chunk",
        _fake_process,
    )

    result = market_data_ratios_weekly.process_ratio_chunk.function(payload)

    assert result == expected_summary


@pytest.mark.unit
def test_finalize_ratio_load_delegates_to_orchestrator(monkeypatch):
    chunk_results = [{"chunk_index": 1, "rows_loaded": 1}]
    expected = {
        "chunks": 1,
        "assets": 1,
        "records_extracted": 1,
        "rows_loaded": 1,
        "alert_mode": False,
        "failed_symbols": 0,
        "failed_rows": 0,
        "failed_batches": 0,
    }

    def _fake_finalize(results):
        assert results == chunk_results
        return expected

    monkeypatch.setattr(
        market_data_ratios_weekly.ratios_orchestrator,
        "finalize_ratio_load",
        _fake_finalize,
    )

    assert (
        market_data_ratios_weekly.finalize_ratio_load.function(chunk_results)
        == expected
    )
