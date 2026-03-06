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
class TestFetchPreciousMetals:
    """Unit tests for fetch_precious_metals task."""

    @patch("dags.assets_dimension_etl.get_clickhouse_client")
    @patch("yfinance.Ticker")
    def test_inserts_xau_xag_with_commodity_class(
        self, mock_yf_ticker, mock_get_client
    ):
        from dags.assets_dimension_etl import fetch_precious_metals

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        # Return predictable metadata for both provider symbols.
        def make_ticker(symbol):
            ticker = MagicMock()
            ticker.info = {
                "longName": "Provider Name",
                "exchange": "CMX",
            }
            return ticker

        mock_yf_ticker.side_effect = make_ticker

        inserted_frames = []

        def capture_insert(table_name, df):
            inserted_frames.append((table_name, df.copy()))

        mock_client.insert_df.side_effect = capture_insert

        result = fetch_precious_metals()

        assert result == 2
        assert len(inserted_frames) == 1

        table_name, df = inserted_frames[0]
        assert table_name == "portfolios_tracker_dw.dim_assets"
        assert len(df) == 2

        symbols = set(df["symbol"].tolist())
        assert symbols == {"XAU", "XAG"}
        assert set(df["asset_class"].tolist()) == {"COMMODITY"}
        assert set(df["currency"].tolist()) == {"USD"}
        assert set(df["sector"].tolist()) == {"Precious Metals"}

        xau_row = df[df["symbol"] == "XAU"].iloc[0]
        xag_row = df[df["symbol"] == "XAG"].iloc[0]

        assert xau_row["external_api_metadata"]["source_symbol"] == "GC=F"
        assert xag_row["external_api_metadata"]["source_symbol"] == "SI=F"
        assert xau_row["external_api_metadata"]["commodity_type"] == "precious_metal"
        assert xag_row["external_api_metadata"]["unit"] == "troy_ounce"

    @patch("dags.assets_dimension_etl.get_clickhouse_client")
    @patch("yfinance.Ticker")
    def test_falls_back_when_yfinance_enrichment_fails(
        self, mock_yf_ticker, mock_get_client
    ):
        from dags.assets_dimension_etl import fetch_precious_metals

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_yf_ticker.side_effect = Exception("provider unavailable")

        inserted_frames = []

        def capture_insert(table_name, df):
            inserted_frames.append((table_name, df.copy()))

        mock_client.insert_df.side_effect = capture_insert

        result = fetch_precious_metals()

        assert result == 2
        assert len(inserted_frames) == 1

        _, df = inserted_frames[0]
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        # Fallback names are explicitly set by mapping.
        assert "Gold (XAU/USD)" in df["name_en"].tolist()
        assert "Silver (XAG/USD)" in df["name_en"].tolist()
