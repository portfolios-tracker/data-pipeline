from typing import get_type_hints

import pandas as pd
import pytest

from dags.etl_modules.orchestrators import ratios_orchestrator


class _RatioProviderStub:
    def fetch_ratios(self, symbol, asset_id):
        if symbol == "ERR":
            raise RuntimeError("fetch failed")
        return pd.DataFrame(
            [
                {
                    "asset_id": asset_id,
                    "fiscal_date": "2025-12-31",
                    "year": 2025,
                    "quarter": 4,
                    "source_provider": "KBS",
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
                    "free_cash_flow": float("inf"),
                }
            ]
        )


class _RepositoryStub:
    def __init__(self, failed_batches=None, fatal_error=None):
        self.failed_batches = failed_batches or []
        self.fatal_error = fatal_error
        self.captured_rows = None

    def upsert_rows(self, *, db_url, query, rows, table_name, batch_size):
        self.captured_rows = rows
        return self.failed_batches, self.fatal_error


@pytest.mark.unit
def test_process_ratio_chunk_preserves_finite_values_and_sanitizes_non_finite():
    provider = _RatioProviderStub()
    repository = _RepositoryStub()
    ratio_columns = (
        "asset_id",
        "fiscal_date",
        "year",
        "quarter",
        "source_provider",
        "pe_ratio",
        "pb_ratio",
        "ps_ratio",
        "p_cashflow_ratio",
        "eps",
        "bvps",
        "market_cap",
        "roe",
        "roa",
        "roic",
        "financial_leverage",
        "dividend_yield",
        "net_profit_margin",
        "debt_to_equity",
        "current_ratio",
        "quick_ratio",
        "interest_coverage",
        "asset_turnover",
        "inventory_turnover",
        "receivable_turnover",
        "revenue_growth",
        "profit_growth",
        "operating_margin",
        "gross_margin",
        "free_cash_flow",
    )
    numeric_columns = (
        "pe_ratio",
        "pb_ratio",
        "ps_ratio",
        "p_cashflow_ratio",
        "eps",
        "bvps",
        "market_cap",
        "roe",
        "roa",
        "roic",
        "financial_leverage",
        "dividend_yield",
        "net_profit_margin",
        "debt_to_equity",
        "current_ratio",
        "quick_ratio",
        "interest_coverage",
        "asset_turnover",
        "inventory_turnover",
        "receivable_turnover",
        "revenue_growth",
        "profit_growth",
        "operating_margin",
        "gross_margin",
        "free_cash_flow",
    )

    summary = ratios_orchestrator.process_ratio_chunk(
        {
            "chunk_index": 1,
            "assets": [
                {"symbol": "BMP", "asset_id": "1816833c-6a63-4441-ab32-8a65660c23da"},
                {"symbol": "ERR", "asset_id": "2"},
            ],
        },
        provider=provider,
        repository=repository,
        db_url="postgres://example.local/test",
        batch_size=100,
        upsert_sql="INSERT INTO market_data.financial_ratios VALUES %s",
        ratio_columns=ratio_columns,
        numeric_sanitize_columns=numeric_columns,
    )

    assert summary["rows_loaded"] == 1
    assert len(summary["failed_symbols"]) == 1
    row = repository.captured_rows[0]
    interest_coverage_idx = ratio_columns.index("interest_coverage")
    inventory_turnover_idx = ratio_columns.index("inventory_turnover")
    free_cash_flow_idx = ratio_columns.index("free_cash_flow")
    assert row[interest_coverage_idx] == pytest.approx(100000.0)
    assert row[inventory_turnover_idx] == pytest.approx(-12000.0)
    assert row[free_cash_flow_idx] is None


@pytest.mark.unit
def test_finalize_ratio_load_raises_only_on_fatal_errors():
    partial_result = ratios_orchestrator.finalize_ratio_load(
        [
            {
                "chunk_index": 1,
                "chunk_assets": 2,
                "records_extracted": 10,
                "rows_prepared": 10,
                "rows_loaded": 9,
                "failed_symbols": [{"symbol": "AAA", "error": "timeout"}],
                "failed_rows": [],
                "failed_batches": [{"batch_index": 1, "size": 1, "error": "db"}],
                "fatal_error": None,
            }
        ]
    )

    assert partial_result["alert_mode"] is True
    assert partial_result["failed_batches"] == 1

    with pytest.raises(RuntimeError):
        ratios_orchestrator.finalize_ratio_load(
            [
                {
                    "chunk_index": 2,
                    "chunk_assets": 1,
                    "records_extracted": 1,
                    "rows_prepared": 1,
                    "rows_loaded": 0,
                    "failed_symbols": [],
                    "failed_rows": [],
                    "failed_batches": [],
                    "fatal_error": "connection error",
                }
            ]
        )


@pytest.mark.unit
def test_public_ratios_orchestrator_interfaces_are_typed():
    list_hints = get_type_hints(ratios_orchestrator.list_ratio_assets)
    chunk_hints = get_type_hints(ratios_orchestrator.chunk_assets)
    process_hints = get_type_hints(ratios_orchestrator.process_ratio_chunk)
    finalize_hints = get_type_hints(ratios_orchestrator.finalize_ratio_load)

    assert "return" in list_hints
    assert {"assets", "chunk_size", "return"} <= set(chunk_hints.keys())
    assert {
        "chunk_payload",
        "db_url",
        "batch_size",
        "upsert_sql",
        "ratio_columns",
        "numeric_sanitize_columns",
        "return",
    } <= set(process_hints.keys())
    assert {"chunk_results", "return"} <= set(finalize_hints.keys())
