"""
Unit tests for dags/ingest_company_intelligence.py

Tests cover:
- build_embedding_text: Text concatenation for embedding
- Batch processing logic
- Rate limiting behaviour (sleep between API calls)
"""

from unittest.mock import MagicMock, Mock, patch

import pytest
from dags.ingest_company_intelligence import build_embedding_text


@pytest.mark.unit
class TestBuildEmbeddingText:
    """Unit tests for the build_embedding_text helper."""

    def test_full_icb_taxonomy_concatenated(self):
        """All four ICB levels are joined with ' > ' separator."""
        result = build_embedding_text(
            company_profile="Công ty sản xuất thép.",
            icb_name2="Công nghiệp",
            icb_name3="Nguyên vật liệu",
            icb_name4="Kim loại",
        )
        assert "Công ty sản xuất thép." in result
        assert "Sector: Công nghiệp > Nguyên vật liệu > Kim loại" in result

    def test_partial_icb_taxonomy_skips_empty_levels(self):
        """Empty ICB levels are omitted from the sector string."""
        result = build_embedding_text(
            company_profile="Profile text.",
            icb_name2="Finance",
            icb_name3="",
            icb_name4="",
        )
        assert "Sector: Finance" in result
        assert ">" not in result

    def test_all_empty_icb_shows_unknown(self):
        """When all ICB fields are empty the sector defaults to 'Unknown'."""
        result = build_embedding_text(
            company_profile="Some profile.",
            icb_name2="",
            icb_name3="",
            icb_name4="",
        )
        assert "Sector: Unknown" in result

    def test_whitespace_only_icb_treated_as_empty(self):
        """ICB fields that are only whitespace are treated as empty."""
        result = build_embedding_text(
            company_profile="Profile.",
            icb_name2="  ",
            icb_name3="  ",
            icb_name4="  ",
        )
        assert "Sector: Unknown" in result

    def test_text_format_has_double_newline_separator(self):
        """Profile and sector are separated by a double newline."""
        result = build_embedding_text(
            company_profile="The profile.",
            icb_name2="Tech",
            icb_name3="Software",
            icb_name4="SaaS",
        )
        assert result == "The profile.\n\nSector: Tech > Software > SaaS"

    def test_empty_company_profile(self):
        """An empty company_profile still produces a valid sector line."""
        result = build_embedding_text(
            company_profile="",
            icb_name2="Energy",
            icb_name3="Oil",
            icb_name4="",
        )
        assert "Sector: Energy > Oil" in result


@pytest.mark.unit
class TestBatchProcessing:
    """Unit tests for batch processing and rate limiting logic."""

    @patch("dags.ingest_company_intelligence.time.sleep")
    @patch("dags.ingest_company_intelligence.get_supabase_client")
    def test_rate_limiting_sleep_called_between_tickers(self, mock_get_supabase, mock_sleep):
        """
        Verifies 0.5s sleep is called after each ticker regardless of success or failure.
        """
        from dags.ingest_company_intelligence import fetch_company_profiles

        mock_supabase = MagicMock()
        mock_get_supabase.return_value = mock_supabase
        mock_supabase.schema.return_value.table.return_value.select.return_value.eq.return_value.eq.return_value.eq.return_value.order.return_value.execute.return_value = Mock(
            data=[
                {"symbol": "VNM", "exchange": "HOSE"},
                {"symbol": "HPG", "exchange": "HOSE"},
                {"symbol": "VCB", "exchange": "HNX"},
            ]
        )

        mock_ti = MagicMock()
        context = {"ti": mock_ti}

        with patch("dags.ingest_company_intelligence.Company") as mock_company_cls:
            mock_company = MagicMock()
            import pandas as pd

            empty_df = pd.DataFrame()
            mock_company.overview.return_value = empty_df
            mock_company_cls.return_value = mock_company

            fetch_company_profiles(**context)

        assert mock_sleep.call_count == 3
        mock_sleep.assert_called_with(0.5)

    @patch("dags.ingest_company_intelligence.get_supabase_client")
    def test_skips_ticker_with_empty_overview(self, mock_get_supabase):
        """Tickers with empty vnstock overview are skipped, not added to profiles."""
        import pandas as pd
        from dags.ingest_company_intelligence import fetch_company_profiles

        mock_supabase = MagicMock()
        mock_get_supabase.return_value = mock_supabase
        mock_supabase.schema.return_value.table.return_value.select.return_value.eq.return_value.eq.return_value.eq.return_value.order.return_value.execute.return_value = Mock(
            data=[{"symbol": "EMPTY", "exchange": "HOSE"}]
        )

        mock_ti = MagicMock()
        context = {"ti": mock_ti}

        with patch("dags.ingest_company_intelligence.Company") as mock_company_cls:
            mock_company = MagicMock()
            mock_company.overview.return_value = pd.DataFrame()
            mock_company_cls.return_value = mock_company

            with patch("dags.ingest_company_intelligence.time.sleep"):
                result = fetch_company_profiles(**context)

        assert result == 0
        pushed_profiles = mock_ti.xcom_push.call_args[1]["value"]
        assert len(pushed_profiles) == 0

    @patch("dags.ingest_company_intelligence.get_supabase_client")
    def test_skips_ticker_with_empty_company_profile_field(self, mock_get_supabase):
        """Tickers whose overview row has an empty company_profile string are skipped."""
        import pandas as pd
        from dags.ingest_company_intelligence import fetch_company_profiles

        mock_supabase = MagicMock()
        mock_get_supabase.return_value = mock_supabase
        mock_supabase.schema.return_value.table.return_value.select.return_value.eq.return_value.eq.return_value.eq.return_value.order.return_value.execute.return_value = Mock(
            data=[{"symbol": "BLANK", "exchange": "HOSE"}]
        )

        mock_ti = MagicMock()
        context = {"ti": mock_ti}

        with patch("dags.ingest_company_intelligence.Company") as mock_company_cls:
            mock_company = MagicMock()
            mock_company.overview.return_value = pd.DataFrame(
                [
                    {
                        "company_profile": "",
                        "icb_name2": "Tech",
                        "icb_name3": "",
                        "icb_name4": "",
                    }
                ]
            )
            mock_company_cls.return_value = mock_company

            with patch("dags.ingest_company_intelligence.time.sleep"):
                result = fetch_company_profiles(**context)

        assert result == 0

    @patch("dags.ingest_company_intelligence.get_supabase_client")
    def test_logs_progress_every_50_tickers(self, mock_get_supabase):
        """Progress is logged at index 49 (50th ticker processed)."""
        import pandas as pd
        from dags.ingest_company_intelligence import fetch_company_profiles

        mock_supabase = MagicMock()
        mock_get_supabase.return_value = mock_supabase
        mock_supabase.schema.return_value.table.return_value.select.return_value.eq.return_value.eq.return_value.eq.return_value.order.return_value.execute.return_value = Mock(
            data=[{"symbol": f"T{i:03d}", "exchange": "HOSE"} for i in range(51)]
        )

        mock_ti = MagicMock()
        context = {"ti": mock_ti}

        with patch("dags.ingest_company_intelligence.Company") as mock_company_cls:
            mock_company = MagicMock()
            mock_company.overview.return_value = pd.DataFrame(
                [
                    {
                        "company_profile": "Profile text",
                        "icb_name2": "A",
                        "icb_name3": "B",
                        "icb_name4": "C",
                    }
                ]
            )
            mock_company_cls.return_value = mock_company

            with patch("dags.ingest_company_intelligence.time.sleep"):
                with patch("dags.ingest_company_intelligence.logger") as mock_logger:
                    fetch_company_profiles(**context)

        info_calls = [str(c) for c in mock_logger.info.call_args_list]
        progress_logs = [c for c in info_calls if "50/" in c or "51/" in c]
        assert len(progress_logs) >= 1

    @patch("dags.ingest_company_intelligence.get_supabase_client")
    @patch("dags.ingest_company_intelligence.genai")
    def test_embeddings_batched_in_groups_of_100(self, mock_genai, mock_get_supabase):
        """Gemini embed_content is called in batches of 100."""
        from dags.ingest_company_intelligence import generate_and_upsert_embeddings

        profiles = [
            {
                "ticker_symbol": f"T{i:03d}",
                "exchange": "HOSE",
                "content_type": "company_profile",
                "company_profile": f"Profile {i}",
                "icb_name2": "Tech",
                "icb_name3": "Software",
                "icb_name4": "SaaS",
            }
            for i in range(250)
        ]

        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = profiles
        context = {"ti": mock_ti}

        def make_embedding_response(model, contents, config=None):
            mock_resp = MagicMock()
            mock_resp.embeddings = [MagicMock(values=[0.1] * 768) for _ in contents]
            return mock_resp

        mock_client = MagicMock()
        mock_genai.Client.return_value = mock_client
        mock_client.models.embed_content.side_effect = make_embedding_response

        mock_supabase = MagicMock()
        mock_get_supabase.return_value = mock_supabase
        mock_supabase.table.return_value.upsert.return_value.execute.return_value = (
            MagicMock()
        )

        with patch.dict("os.environ", {"GEMINI_API_KEY": "test-key"}):
            result = generate_and_upsert_embeddings(**context)

        assert mock_client.models.embed_content.call_count == 3
        assert result == 250

    @patch("dags.ingest_company_intelligence.get_supabase_client")
    @patch("dags.ingest_company_intelligence.genai")
    def test_upsert_batched_in_groups_of_100(self, mock_genai, mock_get_supabase):
        """Supabase upsert is called in batches of 100."""
        from dags.ingest_company_intelligence import generate_and_upsert_embeddings

        profiles = [
            {
                "ticker_symbol": f"T{i:03d}",
                "exchange": "HOSE",
                "content_type": "company_profile",
                "company_profile": f"Profile {i}",
                "icb_name2": "Finance",
                "icb_name3": "Banking",
                "icb_name4": "Commercial",
            }
            for i in range(150)
        ]
        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = profiles
        mock_client = MagicMock()
        mock_genai.Client.return_value = mock_client
        context = {"ti": mock_ti}

        def make_embedding_response(*args, **kwargs):
            contents = kwargs.get("contents", [])
            mock_resp = MagicMock()
            mock_resp.embeddings = [MagicMock(values=[0.0] * 768) for _ in contents]
            return mock_resp

        mock_client.models.embed_content.side_effect = make_embedding_response

        mock_supabase = MagicMock()
        mock_get_supabase.return_value = mock_supabase
        mock_table = mock_supabase.table.return_value
        mock_table.upsert.return_value.execute.return_value = MagicMock()

        with patch.dict("os.environ", {"GEMINI_API_KEY": "test-key"}):
            generate_and_upsert_embeddings(**context)

        assert mock_table.upsert.call_count == 2

    @patch("dags.ingest_company_intelligence.get_supabase_client")
    @patch("dags.ingest_company_intelligence.genai")
    def test_upsert_record_includes_required_fields(
        self, mock_genai, mock_get_supabase
    ):
        """Each upserted record contains all required fields with correct values."""
        from dags.ingest_company_intelligence import generate_and_upsert_embeddings

        profile = {
            "ticker_symbol": "VNM",
            "exchange": "HOSE",
            "content_type": "company_profile",
            "company_profile": "Vinamilk la cong ty sua",
            "icb_name2": "Hàng tiêu dùng",
            "icb_name3": "Thực phẩm",
            "icb_name4": "Sữa",
        }

        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = [profile]
        context = {"ti": mock_ti}
        fake_embedding = [0.42] * 768
        mock_client = MagicMock()
        mock_genai.Client.return_value = mock_client
        mock_resp = MagicMock()
        mock_resp.embeddings = [MagicMock(values=fake_embedding)]
        mock_client.models.embed_content.return_value = mock_resp

        captured_records = []
        mock_supabase = MagicMock()
        mock_get_supabase.return_value = mock_supabase

        def capture_upsert(records, **kwargs):
            captured_records.extend(records)
            return MagicMock(execute=MagicMock(return_value=MagicMock()))

        mock_supabase.table.return_value.upsert.side_effect = capture_upsert

        with patch.dict("os.environ", {"GEMINI_API_KEY": "test-key"}):
            generate_and_upsert_embeddings(**context)

        assert len(captured_records) == 1
        rec = captured_records[0]
        assert rec["ticker_symbol"] == "VNM"
        assert rec["exchange"] == "HOSE"
        assert rec["content_type"] == "company_profile"
        assert rec["embedding"] == fake_embedding
        assert rec["metadata"]["icb_name2"] == "Hàng tiêu dùng"
        assert rec["metadata"]["source"] == "vnstock_vci_gemini"

    @patch("dags.ingest_company_intelligence.get_supabase_client")
    @patch("dags.ingest_company_intelligence.genai")
    def test_empty_profiles_xcom_skips_processing(self, mock_genai, mock_get_supabase):
        """If no profiles in XCom, embeddings generation is skipped and returns 0."""
        from dags.ingest_company_intelligence import generate_and_upsert_embeddings

        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = []
        context = {"ti": mock_ti}

        mock_client = MagicMock()
        mock_genai.Client.return_value = mock_client

        with patch.dict("os.environ", {"GEMINI_API_KEY": "test-key"}):
            result = generate_and_upsert_embeddings(**context)

        assert result == 0
        mock_client.models.embed_content.assert_not_called()
