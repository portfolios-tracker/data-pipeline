"""
Unit tests for dags/assets_dimension_etl.py

Tests cover:
- Precious metals ingestion mapping (XAU/XAG)
- Commodity classification and metadata mapping
"""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


@pytest.mark.unit
class TestFetchVnStocks:
    """Unit tests for fetch_vn_stocks type-based classification."""

    @patch("dags.assets_dimension_etl.delete_vn_non_stock_assets")
    @patch("dags.assets_dimension_etl.upsert_assets_records")
    @patch("vnstock.Listing")
    def test_only_stock_type_is_upserted(
        self, mock_listing_class, mock_upsert_assets, mock_delete_non_stock
    ):
        from dags.assets_dimension_etl import fetch_vn_stocks

        mock_listing = MagicMock()
        mock_listing.symbols_by_exchange.return_value = pd.DataFrame(
            {
                "symbol": ["HPG", "41I1G4000"],
                "organ_name": ["Hoa Phat Group", "VN30 Index Futures 042026"],
                "exchange": ["HOSE", "HNX"],
                "type": ["STOCK", "FU"],
            }
        )
        mock_listing.symbols_by_industries.return_value = pd.DataFrame(
            {
                "symbol": ["HPG"],
                "icb_name2": ["Materials"],
                "icb_name3": ["Steel"],
            }
        )
        mock_listing_class.return_value = mock_listing

        captured_records = []

        def capture_upsert(records):
            captured_records.extend(records)
            return len(records)

        mock_upsert_assets.side_effect = capture_upsert
        mock_delete_non_stock.return_value = 1

        result = fetch_vn_stocks()

        assert result == 1
        assert len(captured_records) == 1
        assert captured_records[0]["symbol"] == "HPG"
        assert captured_records[0]["asset_class"] == "STOCK"
        mock_delete_non_stock.assert_called_once_with(["41I1G4000"])

    @patch("dags.assets_dimension_etl.delete_vn_non_stock_assets")
    @patch("dags.assets_dimension_etl.upsert_assets_records")
    @patch("vnstock.Listing")
    def test_keeps_symbols_when_type_column_absent(
        self, mock_listing_class, mock_upsert_assets, mock_delete_non_stock
    ):
        from dags.assets_dimension_etl import fetch_vn_stocks

        mock_listing = MagicMock()
        mock_listing.symbols_by_exchange.return_value = pd.DataFrame(
            {
                "symbol": ["HPG", "VCB"],
                "organ_name": ["Hoa Phat Group", "Vietcombank"],
                "exchange": ["HOSE", "HOSE"],
            }
        )
        mock_listing.symbols_by_industries.side_effect = Exception("industry unavailable")
        mock_listing_class.return_value = mock_listing

        mock_upsert_assets.side_effect = lambda records: len(records)
        mock_delete_non_stock.return_value = 0

        result = fetch_vn_stocks()
        assert result == 2
        mock_delete_non_stock.assert_called_once_with([])


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
