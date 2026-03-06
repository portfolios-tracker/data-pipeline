"""
Integration tests for dags/sync_assets_to_postgres.py

Tests the payload transformation from ClickHouse rows to Supabase upsert payload,
including market-null handling for non-country assets like commodities.
"""

from unittest.mock import MagicMock, Mock, patch

import pytest


@pytest.mark.integration
class TestSyncAssetsToPostgresIntegration:
    """Integration tests for sync_assets task with mocked infrastructure."""

    @patch("dags.sync_assets_to_postgres.create_client")
    @patch("dags.sync_assets_to_postgres.clickhouse_connect.get_client")
    def test_syncs_commodity_with_null_market_and_metadata(
        self, mock_get_ch_client, mock_create_client
    ):
        from dags.sync_assets_to_postgres import sync_assets

        # Mock ClickHouse query response
        ch_client = MagicMock()
        mock_get_ch_client.return_value = ch_client

        mock_result = Mock()
        mock_result.column_names = [
            "symbol",
            "name_en",
            "name_local",
            "asset_class",
            "market",
            "currency",
            "exchange",
            "sector",
            "industry",
            "logo_url",
            "external_api_metadata",
            "source",
        ]
        mock_result.result_rows = [
            (
                "XAU",
                "Gold (XAU/USD)",
                "Gold",
                "COMMODITY",
                "",
                "USD",
                "CMX",
                "Precious Metals",
                "Commodity",
                "",
                {
                    "source_api": "yfinance",
                    "source_symbol": "GC=F",
                    "commodity_type": "precious_metal",
                    "unit": "troy_ounce",
                },
                "yfinance",
            )
        ]
        ch_client.query.return_value = mock_result

        # Mock Supabase upsert chain
        supabase = MagicMock()
        mock_create_client.return_value = supabase
        supabase.table.return_value.upsert.return_value.execute.return_value = MagicMock()

        with patch.dict(
            "os.environ",
            {
                "SUPABASE_URL": "https://example.supabase.co",
                "SUPABASE_SECRET_OR_SERVICE_ROLE_KEY": "service-role-key",
            },
        ):
            synced_count = sync_assets()

        assert synced_count == 1

        # Validate upsert payload
        upsert_args = supabase.table.return_value.upsert.call_args
        payload = upsert_args[0][0]
        assert len(payload) == 1

        record = payload[0]
        assert record["symbol"] == "XAU"
        assert record["asset_class"] == "COMMODITY"
        # Empty ClickHouse market should map to NULL in Postgres.
        assert record["market"] is None
        assert record["metadata"]["source_symbol"] == "GC=F"
        assert record["metadata"]["unit"] == "troy_ounce"
