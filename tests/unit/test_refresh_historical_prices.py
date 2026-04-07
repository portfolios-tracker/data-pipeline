from unittest.mock import MagicMock

import pytest

from dags.etl_modules.refresh_historical_trigger import (
    ensure_corporate_events_table_exists,
    fetch_tickers_for_refresh,
)


@pytest.mark.unit
class TestEnsureCorporateEventsTableExists:
    def test_returns_without_error_when_corporate_events_exists(self):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (True,)

        ensure_corporate_events_table_exists(mock_cursor)

        executed_sql = mock_cursor.execute.call_args[0][0]
        assert "to_regclass('market_data.corporate_events')" in executed_sql

    def test_raises_when_corporate_events_table_is_missing(self):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (False,)

        with pytest.raises(RuntimeError, match="market_data.corporate_events"):
            ensure_corporate_events_table_exists(mock_cursor)


@pytest.mark.unit
class TestFetchTickersForRefresh:
    def test_queries_events_and_returns_distinct_tickers(self):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [("HPG",), ("VCB",)]

        tickers = fetch_tickers_for_refresh(
            mock_cursor,
            events_lookback_days=14,
        )

        assert tickers == ["HPG", "VCB"]
        executed_sql = mock_cursor.execute.call_args[0][0]
        assert "FROM market_data.corporate_events ce" in executed_sql
        assert "JOIN market_data.assets a" in executed_sql
        assert "market_data.market_data_prices" in executed_sql
        assert mock_cursor.execute.call_args[0][1] == (14,)
