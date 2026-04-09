"""
Integration tests for dags/ingest_company_intelligence.py

Tests the full pipeline flow with mocked external services:
- Mocked vnstock Company API
- Mocked Gemini Embeddings API
- Mocked Supabase upsert

These tests validate the end-to-end data flow without calling real external APIs.
"""

from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import pytest

SAMPLE_PROFILES = [
    {
        "ticker_symbol": "VNM",
        "exchange": "HOSE",
        "content_type": "company_profile",
        "company_profile": (
            "Công ty Cổ phần Sữa Việt Nam (Vinamilk) là doanh nghiệp hàng đầu "
            "trong lĩnh vực sản xuất và kinh doanh sữa tại Việt Nam."
        ),
        "icb_name2": "Hàng tiêu dùng",
        "icb_name3": "Thực phẩm",
        "icb_name4": "Sữa",
    },
    {
        "ticker_symbol": "HPG",
        "exchange": "HOSE",
        "content_type": "company_profile",
        "company_profile": (
            "Tập đoàn Hòa Phát là nhà sản xuất thép lớn nhất Việt Nam "
            "với các nhà máy sản xuất thép xây dựng và thép cuộn cán nóng."
        ),
        "icb_name2": "Công nghiệp",
        "icb_name3": "Nguyên vật liệu",
        "icb_name4": "Kim loại",
    },
]

FAKE_EMBEDDING = [0.01] * 768


@pytest.mark.integration
class TestFetchCompanyProfilesIntegration:
    """Integration tests for the fetch_company_profiles task."""

    @staticmethod
    def _stub_active_assets_query(mock_supabase, rows):
        query = mock_supabase.schema.return_value.table.return_value.select.return_value
        with_status = query.eq.return_value.eq.return_value.eq.return_value
        execute = with_status.order.return_value.execute
        execute.return_value = Mock(data=rows)
        return query

    @patch("dags.ingest_company_intelligence.time.sleep")
    @patch("dags.ingest_company_intelligence.get_supabase_client")
    def test_fetches_profiles_for_active_vn_tickers(
        self, mock_get_supabase, mock_sleep
    ):
        """Full flow: Supabase assets query → vnstock overview → XCom push."""
        from dags.ingest_company_intelligence import fetch_company_profiles

        mock_supabase = MagicMock()
        mock_get_supabase.return_value = mock_supabase
        self._stub_active_assets_query(
            mock_supabase,
            [
                {"symbol": "VNM", "exchange": "HOSE", "metadata": {}},
                {"symbol": "HPG", "exchange": "HOSE", "metadata": {}},
            ],
        )

        mock_ti = MagicMock()
        context = {"ti": mock_ti}

        def fake_company_overview(symbol):
            profiles_map = {
                "VNM": pd.DataFrame(
                    [
                        {
                            "company_profile": "Vinamilk profile",
                            "icb_name2": "Consumer",
                            "icb_name3": "Food",
                            "icb_name4": "Dairy",
                        }
                    ]
                ),
                "HPG": pd.DataFrame(
                    [
                        {
                            "company_profile": "Hoa Phat profile",
                            "icb_name2": "Industry",
                            "icb_name3": "Materials",
                            "icb_name4": "Metals",
                        }
                    ]
                ),
            }
            return profiles_map.get(symbol, pd.DataFrame())

        with patch(
            "dags.ingest_company_intelligence.fetch_company_overview",
            side_effect=fake_company_overview,
        ):
            result = fetch_company_profiles(**context)

        assert result == 2
        mock_ti.xcom_push.assert_called_once()
        pushed = mock_ti.xcom_push.call_args[1]["value"]
        assert len(pushed) == 2
        symbols = [p["ticker_symbol"] for p in pushed]
        assert "VNM" in symbols
        assert "HPG" in symbols

    @patch("dags.ingest_company_intelligence.time.sleep")
    @patch("dags.ingest_company_intelligence.get_supabase_client")
    def test_handles_vnstock_api_failure_gracefully(
        self, mock_get_supabase, mock_sleep
    ):
        """API failures for individual tickers do not abort the pipeline."""
        from dags.ingest_company_intelligence import fetch_company_profiles

        mock_supabase = MagicMock()
        mock_get_supabase.return_value = mock_supabase
        self._stub_active_assets_query(
            mock_supabase,
            [
                {"symbol": "FAIL", "exchange": "HOSE", "metadata": {}},
                {"symbol": "VNM", "exchange": "HOSE", "metadata": {}},
            ],
        )

        mock_ti = MagicMock()
        context = {"ti": mock_ti}

        def maybe_failing_overview(symbol):
            if symbol == "FAIL":
                raise ConnectionError("VCI source error")
            return pd.DataFrame(
                [
                    {
                        "company_profile": "VNM profile",
                        "icb_name2": "Consumer",
                        "icb_name3": "Food",
                        "icb_name4": "Dairy",
                    }
                ]
            )

        with patch(
            "dags.ingest_company_intelligence.fetch_company_overview",
            side_effect=maybe_failing_overview,
        ):
            result = fetch_company_profiles(**context)

        assert result == 1
        pushed = mock_ti.xcom_push.call_args[1]["value"]
        assert pushed[0]["ticker_symbol"] == "VNM"

    @patch("dags.ingest_company_intelligence.time.sleep")
    @patch("dags.ingest_company_intelligence.get_supabase_client")
    def test_correct_supabase_query_filters_vn_stocks(
        self, mock_get_supabase, mock_sleep
    ):
        """Verifies the correct Supabase filters are applied when querying assets."""
        from dags.ingest_company_intelligence import fetch_company_profiles

        mock_supabase = MagicMock()
        mock_get_supabase.return_value = mock_supabase
        builder = self._stub_active_assets_query(mock_supabase, [])

        mock_ti = MagicMock()
        context = {"ti": mock_ti}

        with patch("dags.ingest_company_intelligence.fetch_company_overview"):
            fetch_company_profiles(**context)

        builder.eq.assert_called_once_with("asset_class", "STOCK")
        builder.eq.return_value.eq.assert_called_once_with("market", "VN")
        builder.eq.return_value.eq.return_value.eq.assert_called_once_with(
            "status", "active"
        )


@pytest.mark.integration
class TestFailureModeResilienceIntegration:
    """Regression tests for outage, partial-failure, and cache-unavailable paths."""

    @patch("dags.ingest_company_intelligence.time.sleep")
    @patch("dags.ingest_company_intelligence.get_supabase_client")
    @patch("dags.ingest_company_intelligence.genai")
    def test_provider_outage_is_recorded_without_fatal_chunk_failure(
        self, mock_genai, mock_get_supabase, mock_sleep
    ):
        from dags.ingest_company_intelligence import process_company_intelligence_chunk

        mock_genai.Client.return_value = MagicMock()
        mock_get_supabase.return_value = MagicMock()
        chunk_payload = {
            "chunk_index": 1,
            "tickers": [
                {"symbol": "AAA", "exchange": "HOSE"},
                {"symbol": "BBB", "exchange": "HNX"},
            ],
        }

        with patch.dict("os.environ", {"GEMINI_API_KEY": "test-key"}):
            with patch(
                "dags.ingest_company_intelligence.fetch_company_overview",
                side_effect=ConnectionError("provider outage"),
            ):
                summary = process_company_intelligence_chunk.function(chunk_payload)

        assert summary["fatal_error"] is None
        assert summary["profiles_fetched"] == 0
        assert summary["rows_upserted"] == 0
        assert summary["failed_embedding_batches"] == []
        assert summary["failed_upsert_batches"] == []
        assert [item["symbol"] for item in summary["failed_tickers"]] == ["AAA", "BBB"]

    @patch("dags.ingest_company_intelligence.time.sleep")
    @patch("dags.ingest_company_intelligence.get_supabase_client")
    @patch("dags.ingest_company_intelligence.genai")
    def test_partial_chunk_failure_sets_alert_mode_but_keeps_finalize_green(
        self, mock_genai, mock_get_supabase, mock_sleep
    ):
        from dags.ingest_company_intelligence import (
            finalize_company_intelligence,
            process_company_intelligence_chunk,
        )

        mock_client = MagicMock()
        mock_resp = MagicMock()
        mock_resp.embeddings = [MagicMock(values=FAKE_EMBEDDING)]
        mock_client.models.embed_content.return_value = mock_resp
        mock_genai.Client.return_value = mock_client

        mock_supabase = MagicMock()
        mock_get_supabase.return_value = mock_supabase
        mock_supabase.table.return_value.upsert.return_value.execute.return_value = (
            MagicMock()
        )

        chunk_payload = {
            "chunk_index": 7,
            "tickers": [
                {"symbol": "GOOD", "exchange": "HOSE"},
                {"symbol": "MISS", "exchange": "HOSE"},
            ],
        }

        def mixed_provider(symbol):
            if symbol == "MISS":
                return pd.DataFrame()
            return pd.DataFrame(
                [
                    {
                        "company_profile": "Good profile",
                        "icb_name2": "Consumer",
                        "icb_name3": "Food",
                        "icb_name4": "Dairy",
                    }
                ]
            )

        with patch.dict("os.environ", {"GEMINI_API_KEY": "test-key"}):
            with patch(
                "dags.ingest_company_intelligence.fetch_company_overview",
                side_effect=mixed_provider,
            ):
                summary = process_company_intelligence_chunk.function(chunk_payload)
                final_summary = finalize_company_intelligence.function([summary])

        assert summary["fatal_error"] is None
        assert summary["rows_upserted"] == 1
        assert summary["failed_tickers"] == [
            {"symbol": "MISS", "error": "empty overview"}
        ]
        assert final_summary["rows_upserted"] == 1
        assert final_summary["failed_tickers"] == 1
        assert final_summary["alert_mode"] is True

    @patch("dags.etl_modules.vci_provider._company_type_code", return_value="CT")
    @patch("dags.etl_modules.vci_provider._graphql")
    @patch("dags.etl_modules.vci_provider.get_redis_client")
    def test_fetch_financial_ratios_survives_cache_unavailable(
        self, mock_get_redis_client, mock_graphql, _mock_company_type_code
    ):
        from dags.etl_modules import vci_provider

        cache_client = MagicMock()
        cache_client.get.side_effect = RuntimeError("redis unavailable")
        cache_client.set.side_effect = RuntimeError("redis unavailable")
        mock_get_redis_client.return_value = cache_client

        def fake_graphql(query, variables=None, *, operation=None):
            if "ListFinancialRatio" in query:
                return {
                    "ListFinancialRatio": [
                        {
                            "fieldName": "is_net_sales",
                            "en_Name": "Net Sales",
                            "name": "Doanh thu thuần",
                            "type": "income statement",
                            "comTypeCode": "CT",
                            "order": 1,
                        }
                    ]
                }
            if "CompanyFinancialRatio" in query:
                return {
                    "CompanyFinancialRatio": {
                        "ratio": [
                            {
                                "ticker": "HPG",
                                "yearReport": 2025,
                                "lengthReport": 4,
                                "updateDate": "2025-12-31",
                                "is_net_sales": 1000,
                            }
                        ]
                    }
                }
            raise AssertionError(f"Unexpected query: {query}")

        mock_graphql.side_effect = fake_graphql

        frame = vci_provider.fetch_financial_ratios("HPG", period="Q")

        assert not frame.empty
        assert "Net Sales" in frame.columns
        assert cache_client.get.called
        assert cache_client.set.called


@pytest.mark.integration
class TestGenerateAndUpsertEmbeddingsIntegration:
    """Integration tests for the generate_and_upsert_embeddings task."""

    @patch("dags.ingest_company_intelligence.get_supabase_client")
    @patch("dags.ingest_company_intelligence.genai")
    def test_full_pipeline_produces_correct_embedding_count(
        self, mock_genai, mock_get_supabase
    ):
        """End-to-end: profiles → embeddings → upserted records count matches."""
        from dags.ingest_company_intelligence import generate_and_upsert_embeddings

        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = SAMPLE_PROFILES
        context = {"ti": mock_ti}
        mock_client = MagicMock()
        mock_genai.Client.return_value = mock_client
        mock_resp = MagicMock()
        mock_resp.embeddings = [
            MagicMock(values=FAKE_EMBEDDING) for _ in SAMPLE_PROFILES
        ]
        mock_client.models.embed_content.return_value = mock_resp

        mock_supabase = MagicMock()
        mock_get_supabase.return_value = mock_supabase
        mock_supabase.table.return_value.upsert.return_value.execute.return_value = (
            MagicMock()
        )

        with patch.dict("os.environ", {"GEMINI_API_KEY": "test-key"}):
            result = generate_and_upsert_embeddings(**context)

        assert result == len(SAMPLE_PROFILES)

    @patch("dags.ingest_company_intelligence.get_supabase_client")
    @patch("dags.ingest_company_intelligence.genai")
    def test_uses_gemini_embedding_model(self, mock_genai, mock_get_supabase):
        """Verifies the correct Gemini embedding model is used."""
        from dags.ingest_company_intelligence import generate_and_upsert_embeddings

        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = SAMPLE_PROFILES
        context = {"ti": mock_ti}
        mock_client = MagicMock()
        mock_genai.Client.return_value = mock_client
        mock_resp = MagicMock()
        mock_resp.embeddings = [
            MagicMock(values=FAKE_EMBEDDING) for _ in SAMPLE_PROFILES
        ]
        mock_client.models.embed_content.return_value = mock_resp

        mock_supabase = MagicMock()
        mock_get_supabase.return_value = mock_supabase
        mock_supabase.table.return_value.upsert.return_value.execute.return_value = (
            MagicMock()
        )

        with patch.dict("os.environ", {"GEMINI_API_KEY": "test-key"}):
            generate_and_upsert_embeddings(**context)

        call_kwargs = mock_client.models.embed_content.call_args
        assert call_kwargs[1]["model"] == "text-embedding-004"

    @patch("dags.ingest_company_intelligence.get_supabase_client")
    @patch("dags.ingest_company_intelligence.genai")
    def test_embedding_content_uses_full_concatenated_text(
        self, mock_genai, mock_get_supabase
    ):
        """Embedding text includes company_profile and sector taxonomy."""
        from dags.ingest_company_intelligence import generate_and_upsert_embeddings

        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = [SAMPLE_PROFILES[0]]
        context = {"ti": mock_ti}

        captured_inputs = []

        def capture_embed(model, contents, config=None):
            captured_inputs.extend(contents)
            mock_resp = MagicMock()
            mock_resp.embeddings = [MagicMock(values=FAKE_EMBEDDING)]
            return mock_resp

        mock_client = MagicMock()
        mock_genai.Client.return_value = mock_client
        mock_client.models.embed_content.side_effect = capture_embed

        mock_supabase = MagicMock()
        mock_get_supabase.return_value = mock_supabase
        mock_supabase.table.return_value.upsert.return_value.execute.return_value = (
            MagicMock()
        )

        with patch.dict("os.environ", {"GEMINI_API_KEY": "test-key"}):
            generate_and_upsert_embeddings(**context)

        assert len(captured_inputs) == 1
        text = captured_inputs[0]
        assert SAMPLE_PROFILES[0]["company_profile"] in text
        assert "Hàng tiêu dùng" in text
        assert "Sector:" in text

    @patch("dags.ingest_company_intelligence.get_supabase_client")
    @patch("dags.ingest_company_intelligence.genai")
    def test_gemini_failure_raises_exception(self, mock_genai, mock_get_supabase):
        """A Gemini API failure raises an exception and stops the task."""
        from dags.ingest_company_intelligence import generate_and_upsert_embeddings

        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = SAMPLE_PROFILES
        context = {"ti": mock_ti}
        mock_client = MagicMock()
        mock_genai.Client.return_value = mock_client
        mock_client.models.embed_content.side_effect = Exception("Gemini rate limit")

        mock_supabase = MagicMock()
        mock_get_supabase.return_value = mock_supabase

        with patch.dict("os.environ", {"GEMINI_API_KEY": "test-key"}):
            with pytest.raises(Exception, match="Gemini rate limit"):
                generate_and_upsert_embeddings(**context)

    @patch("dags.ingest_company_intelligence.get_supabase_client")
    @patch("dags.ingest_company_intelligence.genai")
    def test_supabase_upsert_failure_raises_exception(
        self, mock_genai, mock_get_supabase
    ):
        """A Supabase upsert failure raises an exception and stops the task."""
        from dags.ingest_company_intelligence import generate_and_upsert_embeddings

        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = [SAMPLE_PROFILES[0]]
        mock_client = MagicMock()
        mock_genai.Client.return_value = mock_client
        mock_resp = MagicMock()
        mock_resp.embeddings = [MagicMock(values=FAKE_EMBEDDING)]
        mock_client.models.embed_content.return_value = mock_resp
        context = {"ti": mock_ti}

        mock_supabase = MagicMock()
        mock_get_supabase.return_value = mock_supabase
        mock_supabase.table.return_value.upsert.return_value.execute.side_effect = (
            Exception("Supabase connection refused")
        )

        with patch.dict("os.environ", {"GEMINI_API_KEY": "test-key"}):
            with pytest.raises(Exception, match="Supabase connection refused"):
                generate_and_upsert_embeddings(**context)


@pytest.mark.integration
class TestBackfillDimAssetsDescriptionIntegration:
    """Integration tests for the legacy no-op backfill task."""

    def test_backfill_is_noop_even_with_profiles(self):
        """Backfill is intentionally disabled after Supabase migration."""
        from dags.ingest_company_intelligence import backfill_dim_assets_description

        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = SAMPLE_PROFILES
        context = {"ti": mock_ti}

        result = backfill_dim_assets_description(**context)
        assert result == 0

    def test_skips_backfill_when_no_profiles_available(self):
        """If no profiles in XCom, backfill is skipped and returns 0."""
        from dags.ingest_company_intelligence import backfill_dim_assets_description

        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = []
        context = {"ti": mock_ti}

        result = backfill_dim_assets_description(**context)

        assert result == 0
