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

    @patch("dags.ingest_company_intelligence.time.sleep")
    @patch("dags.ingest_company_intelligence.get_supabase_client")
    def test_fetches_profiles_for_active_vn_tickers(self, mock_get_supabase, mock_sleep):
        """Full flow: Supabase assets query → vnstock overview → XCom push."""
        from dags.ingest_company_intelligence import fetch_company_profiles

        mock_supabase = MagicMock()
        mock_get_supabase.return_value = mock_supabase
        mock_supabase.table.return_value.select.return_value.eq.return_value.eq.return_value.order.return_value.execute.return_value = Mock(
            data=[
                {"symbol": "VNM", "exchange": "HOSE"},
                {"symbol": "HPG", "exchange": "HOSE"},
            ]
        )

        mock_ti = MagicMock()
        context = {"ti": mock_ti}

        def fake_company_overview(symbol, source):
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

        with patch("dags.ingest_company_intelligence.Company") as mock_company_cls:

            def make_company(symbol, source):
                obj = MagicMock()
                obj.overview.return_value = fake_company_overview(symbol, source)
                return obj

            mock_company_cls.side_effect = make_company

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
    def test_handles_vnstock_api_failure_gracefully(self, mock_get_supabase, mock_sleep):
        """API failures for individual tickers do not abort the pipeline."""
        from dags.ingest_company_intelligence import fetch_company_profiles

        mock_supabase = MagicMock()
        mock_get_supabase.return_value = mock_supabase
        mock_supabase.table.return_value.select.return_value.eq.return_value.eq.return_value.order.return_value.execute.return_value = Mock(
            data=[
                {"symbol": "FAIL", "exchange": "HOSE"},
                {"symbol": "VNM", "exchange": "HOSE"},
            ]
        )

        mock_ti = MagicMock()
        context = {"ti": mock_ti}

        with patch("dags.ingest_company_intelligence.Company") as mock_company_cls:

            def make_company(symbol, source):
                obj = MagicMock()
                if symbol == "FAIL":
                    obj.overview.side_effect = ConnectionError("VCI source error")
                else:
                    obj.overview.return_value = pd.DataFrame(
                        [
                            {
                                "company_profile": "VNM profile",
                                "icb_name2": "Consumer",
                                "icb_name3": "Food",
                                "icb_name4": "Dairy",
                            }
                        ]
                    )
                return obj

            mock_company_cls.side_effect = make_company

            result = fetch_company_profiles(**context)

        assert result == 1
        pushed = mock_ti.xcom_push.call_args[1]["value"]
        assert pushed[0]["ticker_symbol"] == "VNM"

    @patch("dags.ingest_company_intelligence.time.sleep")
    @patch("dags.ingest_company_intelligence.get_supabase_client")
    def test_correct_supabase_query_filters_vn_stocks(self, mock_get_supabase, mock_sleep):
        """Verifies the correct Supabase filters are applied when querying assets."""
        from dags.ingest_company_intelligence import fetch_company_profiles

        mock_supabase = MagicMock()
        mock_get_supabase.return_value = mock_supabase
        builder = mock_supabase.table.return_value.select.return_value
        builder.eq.return_value.eq.return_value.order.return_value.execute.return_value = Mock(data=[])

        mock_ti = MagicMock()
        context = {"ti": mock_ti}

        with patch("dags.ingest_company_intelligence.Company"):
            fetch_company_profiles(**context)

        eq_calls = builder.eq.call_args_list
        nested_eq_calls = builder.eq.return_value.eq.call_args_list
        assert any(c.args == ("asset_class", "STOCK") for c in eq_calls)
        assert any(c.args == ("market", "VN") for c in nested_eq_calls)


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
        """The text sent for embedding includes both company_profile and sector taxonomy."""
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
