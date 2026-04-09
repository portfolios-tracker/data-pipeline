from datetime import date
from typing import get_type_hints

import pandas as pd
import pytest

from dags.etl_modules.orchestrators import fundamentals_orchestrator


class _FundamentalsProviderStub:
    def list_assets(self):
        return [
            {"symbol": "AAA", "asset_id": "1"},
            {"symbol": "ERR", "asset_id": "2"},
        ]

    def fetch_income_statement(self, symbol, asset_id):
        if symbol == "ERR":
            raise RuntimeError("fetch income failed")
        return pd.DataFrame(
            [
                {
                    "asset_id": asset_id,
                    "fiscal_date": "2025-12-31",
                    "year": 2025,
                    "quarter": 4,
                    "source_provider": "KBS",
                    "revenue": 100.0,
                    "cost_of_goods_sold": 80.0,
                    "gross_profit": 20.0,
                    "operating_profit": 10.0,
                    "net_profit_post_tax": 8.0,
                    "selling_expenses": 2.0,
                    "admin_expenses": 1.0,
                    "financial_income": 0.5,
                    "financial_expenses": 0.2,
                    "other_income": 0.1,
                    "other_expenses": 0.1,
                    "ebitda": 12.0,
                }
            ]
        )

    def fetch_balance_sheet(self, symbol, asset_id):
        if symbol == "ERR":
            raise RuntimeError("fetch balance failed")
        return pd.DataFrame(
            [
                {
                    "asset_id": asset_id,
                    "fiscal_date": "2025-12-31",
                    "year": 2025,
                    "quarter": 4,
                    "source_provider": "KBS",
                    "total_assets": 200.0,
                    "total_liabilities": 120.0,
                    "total_equity": 80.0,
                    "cash_and_equivalents": 30.0,
                    "short_term_assets": 90.0,
                    "long_term_assets": 110.0,
                    "short_term_liabilities": 50.0,
                    "long_term_liabilities": 70.0,
                }
            ]
        )


class _RepositoryStub:
    def __init__(self, failed_batches=None, fatal_error=None):
        self.failed_batches = failed_batches or []
        self.fatal_error = fatal_error
        self.captured_rows = None
        self.captured_table = None

    def upsert_rows(self, *, db_url, query, rows, table_name, batch_size):
        self.captured_rows = rows
        self.captured_table = table_name
        return self.failed_batches, self.fatal_error


INCOME_COLUMNS = (
    "asset_id",
    "fiscal_date",
    "year",
    "quarter",
    "source_provider",
    "revenue",
    "cost_of_goods_sold",
    "gross_profit",
    "operating_profit",
    "net_profit_post_tax",
    "selling_expenses",
    "admin_expenses",
    "financial_income",
    "financial_expenses",
    "other_income",
    "other_expenses",
    "ebitda",
)

BALANCE_COLUMNS = (
    "asset_id",
    "fiscal_date",
    "year",
    "quarter",
    "source_provider",
    "total_assets",
    "total_liabilities",
    "total_equity",
    "cash_and_equivalents",
    "short_term_assets",
    "long_term_assets",
    "short_term_liabilities",
    "long_term_liabilities",
)


@pytest.mark.unit
def test_extract_income_statements_collects_records_and_partial_failures():
    summary = fundamentals_orchestrator.extract_income_statements(
        provider=_FundamentalsProviderStub()
    )

    assert len(summary["records"]) == 1
    assert summary["records"][0]["asset_id"] == "1"
    assert len(summary["failed_symbols"]) == 1
    assert summary["failed_symbols"][0]["symbol"] == "ERR"


@pytest.mark.unit
def test_load_income_statements_handles_row_and_batch_failures():
    repository = _RepositoryStub(
        failed_batches=[{"batch_index": 1, "size": 1, "error": "db failed"}]
    )

    summary = fundamentals_orchestrator.load_income_statements(
        {
            "records": [
                {
                    "asset_id": "1",
                    "fiscal_date": "2025-12-31",
                    "year": 2025,
                    "quarter": 4,
                    "source_provider": "KBS",
                    "revenue": 1,
                    "cost_of_goods_sold": 1,
                    "gross_profit": 1,
                    "operating_profit": 1,
                    "net_profit_post_tax": 1,
                    "selling_expenses": 1,
                    "admin_expenses": 1,
                    "financial_income": 1,
                    "financial_expenses": 1,
                    "other_income": 1,
                    "other_expenses": 1,
                    "ebitda": 1,
                },
                {"asset_id": "2", "fiscal_date": object()},
            ],
            "failed_symbols": [{"symbol": "ERR", "error": "fetch failed"}],
        },
        provider=_FundamentalsProviderStub(),
        repository=repository,
        db_url="postgres://example.local/test",
        batch_size=100,
        upsert_sql="INSERT INTO market_data.income_statements VALUES %s",
        income_columns=INCOME_COLUMNS,
    )

    assert summary["records_input"] == 2
    assert summary["rows_prepared"] == 1
    assert summary["rows_loaded"] == 0
    assert len(summary["failed_rows"]) == 1
    assert summary["failed_batches"] == [
        {"batch_index": 1, "size": 1, "error": "db failed"}
    ]
    assert repository.captured_rows is not None
    assert repository.captured_table == "market_data.income_statements"


@pytest.mark.unit
def test_load_balance_sheets_parses_date_values():
    repository = _RepositoryStub()
    summary = fundamentals_orchestrator.load_balance_sheets(
        {
            "records": [
                {
                    "asset_id": "1",
                    "fiscal_date": date(2025, 12, 31),
                    "year": 2025,
                    "quarter": 4,
                    "source_provider": "KBS",
                    "total_assets": 1,
                    "total_liabilities": 1,
                    "total_equity": 1,
                    "cash_and_equivalents": 1,
                    "short_term_assets": 1,
                    "long_term_assets": 1,
                    "short_term_liabilities": 1,
                    "long_term_liabilities": 1,
                }
            ],
            "failed_symbols": [],
        },
        provider=_FundamentalsProviderStub(),
        repository=repository,
        db_url="postgres://example.local/test",
        batch_size=100,
        upsert_sql="INSERT INTO market_data.balance_sheets VALUES %s",
        balance_columns=BALANCE_COLUMNS,
    )

    assert summary["rows_loaded"] == 1
    assert repository.captured_rows[0][1].isoformat() == "2025-12-31"


@pytest.mark.unit
def test_finalize_fundamentals_load_raises_when_write_fails():
    with pytest.raises(RuntimeError):
        fundamentals_orchestrator.finalize_fundamentals_load(
            {
                "records_input": 8,
                "rows_prepared": 8,
                "rows_loaded": 6,
                "failed_symbols": [],
                "failed_rows": [],
                "failed_batches": [
                    {"batch_index": 1, "size": 2, "error": "db timeout"}
                ],
            },
            {
                "records_input": 8,
                "rows_prepared": 8,
                "rows_loaded": 8,
                "failed_symbols": [],
                "failed_rows": [],
                "failed_batches": [],
            },
        )


@pytest.mark.unit
def test_public_fundamentals_orchestrator_interfaces_are_typed():
    extract_income_hints = get_type_hints(
        fundamentals_orchestrator.extract_income_statements
    )
    load_income_hints = get_type_hints(fundamentals_orchestrator.load_income_statements)
    extract_balance_hints = get_type_hints(
        fundamentals_orchestrator.extract_balance_sheets
    )
    load_balance_hints = get_type_hints(fundamentals_orchestrator.load_balance_sheets)
    finalize_hints = get_type_hints(
        fundamentals_orchestrator.finalize_fundamentals_load
    )

    assert {"provider", "return"} <= set(extract_income_hints.keys())
    assert {
        "data",
        "provider",
        "repository",
        "db_url",
        "batch_size",
        "upsert_sql",
        "income_columns",
        "return",
    } <= set(load_income_hints.keys())
    assert {"provider", "return"} <= set(extract_balance_hints.keys())
    assert {
        "data",
        "provider",
        "repository",
        "db_url",
        "batch_size",
        "upsert_sql",
        "balance_columns",
        "return",
    } <= set(load_balance_hints.keys())
    assert {"income_summary", "balance_summary", "return"} <= set(finalize_hints.keys())
