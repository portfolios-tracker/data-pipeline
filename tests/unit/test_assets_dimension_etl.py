"""
Unit tests for dags/assets_dimension_etl.py

Tests cover:
- Precious metals ingestion mapping (XAU/XAG)
- Commodity classification and metadata mapping
"""

from unittest.mock import MagicMock, patch

import pytest


@pytest.mark.unit
class TestFetchPreciousMetals:
    """Unit tests for fetch_precious_metals task."""

    @patch("dags.assets_dimension_etl.upsert_assets_records")
    @patch("yfinance.Ticker")
    def test_inserts_xau_xag_with_commodity_class(
        self, mock_yf_ticker, mock_upsert_assets
    ):
        from dags.assets_dimension_etl import fetch_precious_metals

        # Return predictable metadata for both provider symbols.
        def make_ticker(symbol):
            ticker = MagicMock()
            ticker.info = {
                "longName": "Provider Name",
                "exchange": "CMX",
            }
            return ticker

        mock_yf_ticker.side_effect = make_ticker
        captured_records = []

        def capture_upsert(records):
            captured_records.extend(records)
            return len(records)

        mock_upsert_assets.side_effect = capture_upsert

        result = fetch_precious_metals()

        assert result == 2
        assert len(captured_records) == 2

        symbols = {row["symbol"] for row in captured_records}
        assert symbols == {"XAU", "XAG"}
        assert {row["asset_class"] for row in captured_records} == {"COMMODITY"}
        assert {row["currency"] for row in captured_records} == {"USD"}
        assert {row["sector"] for row in captured_records} == {"Precious Metals"}

        xau_row = next(row for row in captured_records if row["symbol"] == "XAU")
        xag_row = next(row for row in captured_records if row["symbol"] == "XAG")

        assert xau_row["external_api_metadata"]["source_symbol"] == "GC=F"
        assert xag_row["external_api_metadata"]["source_symbol"] == "SI=F"
        assert xau_row["external_api_metadata"]["commodity_type"] == "precious_metal"
        assert xag_row["external_api_metadata"]["unit"] == "troy_ounce"

    @patch("dags.assets_dimension_etl.upsert_assets_records")
    @patch("yfinance.Ticker")
    def test_falls_back_when_yfinance_enrichment_fails(
        self, mock_yf_ticker, mock_upsert_assets
    ):
        from dags.assets_dimension_etl import fetch_precious_metals

        mock_yf_ticker.side_effect = Exception("provider unavailable")
        captured_records = []

        def capture_upsert(records):
            captured_records.extend(records)
            return len(records)

        mock_upsert_assets.side_effect = capture_upsert

        result = fetch_precious_metals()

        assert result == 2
        assert len(captured_records) == 2
        # Fallback names are explicitly set by mapping.
        assert "Gold (XAU/USD)" in [row["name_en"] for row in captured_records]
        assert "Silver (XAG/USD)" in [row["name_en"] for row in captured_records]
