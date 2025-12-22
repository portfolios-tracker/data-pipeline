"""
Unit tests for dags/etl_modules/notifications.py

Tests cover:
- send_telegram_message: Core Telegram notification function
- send_success_notification: Success callback wrapper
- send_failure_notification: Failure callback wrapper
- send_telegram_news_summary: News digest with AI summary
- get_latest_stock_data: ClickHouse data retrieval
- summarize_news_with_gemini: AI summary generation
"""

import pytest
import responses
from unittest.mock import patch, Mock, MagicMock
from datetime import datetime

# Import functions to test
from dags.etl_modules.notifications import (
    send_telegram_message,
    send_success_notification,
    send_failure_notification,
    send_telegram_news_summary,
    get_latest_stock_data,
    summarize_news_with_gemini,
)

from tests.mocks.api_responses import (
    mock_telegram_success,
    mock_telegram_error,
    mock_gemini_success,
    get_telegram_url,
    get_gemini_url,
)


# ============================================================================
# Tests for send_telegram_message()
# ============================================================================


@pytest.mark.unit
class TestSendTelegramMessage:
    """Test suite for send_telegram_message function."""

    @responses.activate
    def test_success_notification_sent(
        self, sample_airflow_context, mock_environment_variables
    ):
        """Test that success notification is properly formatted and sent."""
        # Setup mock response
        token = mock_environment_variables["TELEGRAM_BOT_TOKEN"]
        responses.add(
            responses.POST,
            get_telegram_url(token),
            json=mock_telegram_success(),
            status=200,
        )

        # Execute
        send_telegram_message(sample_airflow_context, status="SUCCESS")

        # Assert
        assert len(responses.calls) == 1
        request_body = responses.calls[0].request.body
        assert b"DAG Success" in request_body
        assert b"test_dag" in request_body

    @responses.activate
    def test_failure_notification_with_error(self, mock_environment_variables):
        """Test that failure notification includes error details."""
        # Setup mock response
        token = mock_environment_variables["TELEGRAM_BOT_TOKEN"]
        responses.add(
            responses.POST,
            get_telegram_url(token),
            json=mock_telegram_success(),
            status=200,
        )

        # Create context with failure details
        context = {
            "dag": Mock(dag_id="test_dag"),
            "task_instance": Mock(task_id="failed_task"),
            "run_id": "manual_123",
            "logical_date": datetime(2024, 12, 22, 10, 0),
            "exception": "ValueError: Test error",
        }

        # Execute
        send_telegram_message(context, status="FAILED")

        # Assert
        assert len(responses.calls) == 1
        request_body = responses.calls[0].request.body
        assert b"DAG Failed" in request_body
        assert b"failed_task" in request_body
        assert b"ValueError" in request_body

    def test_missing_credentials_skips_notification(
        self, sample_airflow_context, monkeypatch
    ):
        """Test that missing Telegram credentials are handled gracefully."""
        # Remove credentials
        monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
        monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)

        # Should not raise exception
        send_telegram_message(sample_airflow_context, status="SUCCESS")

    @responses.activate
    def test_api_error_logged(self, sample_airflow_context, mock_environment_variables):
        """Test that Telegram API errors are logged without crashing."""
        # Setup mock error response
        token = mock_environment_variables["TELEGRAM_BOT_TOKEN"]
        responses.add(
            responses.POST,
            get_telegram_url(token),
            json=mock_telegram_error(),
            status=400,
        )

        # Should not raise exception
        send_telegram_message(sample_airflow_context, status="SUCCESS")


# ============================================================================
# Tests for callback wrappers
# ============================================================================


@pytest.mark.unit
class TestNotificationCallbacks:
    """Test suite for success/failure callback wrappers."""

    @patch("dags.etl_modules.notifications.send_telegram_message")
    def test_success_callback(self, mock_send, sample_airflow_context):
        """Test that send_success_notification calls send_telegram_message with SUCCESS."""
        send_success_notification(sample_airflow_context)

        mock_send.assert_called_once_with(sample_airflow_context, status="SUCCESS")

    @patch("dags.etl_modules.notifications.send_telegram_message")
    def test_failure_callback(self, mock_send, sample_airflow_context):
        """Test that send_failure_notification calls send_telegram_message with FAILED."""
        send_failure_notification(sample_airflow_context)

        mock_send.assert_called_once_with(sample_airflow_context, status="FAILED")


# ============================================================================
# Tests for send_telegram_news_summary()
# ============================================================================


@pytest.mark.unit
class TestSendTelegramNewsSummary:
    """Test suite for send_telegram_news_summary function."""

    @responses.activate
    @patch("dags.etl_modules.notifications.summarize_news_with_gemini")
    def test_sends_ai_summary_when_available(
        self, mock_gemini, mock_environment_variables
    ):
        """Test that AI summary is used when Gemini returns content."""
        # Setup mocks
        mock_gemini.return_value = "**AI Generated Summary**\n\nMarket analysis..."

        token = mock_environment_variables["TELEGRAM_BOT_TOKEN"]
        responses.add(
            responses.POST,
            get_telegram_url(token),
            json=mock_telegram_success(),
            status=200,
        )

        news_data = [
            {
                "ticker": "HPG",
                "title": "Test news article",
                "publish_date": datetime(2024, 12, 20),
                "price_impact_1d": 2.5,
            }
        ]

        # Execute
        send_telegram_news_summary(news_data)

        # Assert
        assert len(responses.calls) == 1
        request_body = responses.calls[0].request.body
        assert b"AI Market News Summary" in request_body
        assert b"Powered by Gemini AI" in request_body

    @responses.activate
    @patch("dags.etl_modules.notifications.summarize_news_with_gemini")
    def test_fallback_to_basic_summary(self, mock_gemini, mock_environment_variables):
        """Test that basic summary is used when Gemini fails."""
        # Setup mocks
        mock_gemini.return_value = None

        token = mock_environment_variables["TELEGRAM_BOT_TOKEN"]
        responses.add(
            responses.POST,
            get_telegram_url(token),
            json=mock_telegram_success(),
            status=200,
        )

        news_data = [
            {
                "ticker": "HPG",
                "title": "Test news article",
                "publish_date": datetime(2024, 12, 20),
                "price_impact_1d": 2.5,
            }
        ]

        # Execute
        send_telegram_news_summary(news_data)

        # Assert
        assert len(responses.calls) == 1
        request_body = responses.calls[0].request.body
        assert b"Market News Summary" in request_body
        assert b"Gemini" not in request_body  # Should not mention AI

    def test_empty_news_data_handled(self, mock_environment_variables):
        """Test that empty news data is handled gracefully."""
        # Should not raise exception
        send_telegram_news_summary([])
        send_telegram_news_summary(None)

    @responses.activate
    @patch("dags.etl_modules.notifications.summarize_news_with_gemini")
    def test_long_message_truncated(self, mock_gemini, mock_environment_variables):
        """Test that messages exceeding Telegram limit are truncated."""
        # Generate very long summary
        mock_gemini.return_value = "A" * 5000  # Exceeds 4096 limit

        token = mock_environment_variables["TELEGRAM_BOT_TOKEN"]
        responses.add(
            responses.POST,
            get_telegram_url(token),
            json=mock_telegram_success(),
            status=200,
        )

        news_data = [{"ticker": "HPG", "title": "Test"}]

        # Execute
        send_telegram_news_summary(news_data)

        # Assert message was truncated
        assert len(responses.calls) == 1
        request_body = responses.calls[0].request.body
        assert b"message truncated" in request_body


# ============================================================================
# Tests for get_latest_stock_data()
# ============================================================================


@pytest.mark.unit
class TestGetLatestStockData:
    """Test suite for get_latest_stock_data function."""

    @patch("dags.etl_modules.notifications.clickhouse_connect.get_client")
    def test_successful_data_fetch(self, mock_get_client, mock_environment_variables):
        """Test successful fetching of stock data from ClickHouse."""
        # Setup mock client
        mock_client = MagicMock()

        # Mock market data query
        mock_market_result = Mock()
        mock_market_result.result_rows = [
            (
                "HPG",
                "2024-12-22",
                25500.0,
                5000000,
                2.5,
                25000.0,
                24000.0,
                45.2,
                100.5,
                95.3,
                5.2,
                8.5,
                "Steel",
                "Basic Materials",
            )
        ]

        # Mock valuation query
        mock_val_result = Mock()
        mock_val_result.result_rows = [(15.5, 18.5, 12.5, 0.5, 10.2, 5000)]

        mock_client.query.side_effect = [mock_market_result, mock_val_result]
        mock_get_client.return_value = mock_client

        # Execute
        result = get_latest_stock_data(["HPG"])

        # Assert
        assert "HPG" in result
        assert result["HPG"]["close"] == 25500.0
        assert result["HPG"]["rsi_14"] == 45.2
        assert result["HPG"]["pe_ratio"] == 15.5
        assert result["HPG"]["sector"] == "Steel"

    @patch("dags.etl_modules.notifications.clickhouse_connect.get_client")
    def test_empty_result_handled(self, mock_get_client, mock_environment_variables):
        """Test handling of empty query results."""
        mock_client = MagicMock()
        mock_result = Mock()
        mock_result.result_rows = []
        mock_client.query.return_value = mock_result
        mock_get_client.return_value = mock_client

        result = get_latest_stock_data(["INVALID"])

        assert result == {}

    @patch("dags.etl_modules.notifications.clickhouse_connect.get_client")
    def test_connection_error_handled(
        self, mock_get_client, mock_environment_variables
    ):
        """Test that connection errors are handled gracefully."""
        mock_get_client.side_effect = Exception("Connection failed")

        result = get_latest_stock_data(["HPG"])

        assert result == {}


# ============================================================================
# Tests for summarize_news_with_gemini()
# ============================================================================


@pytest.mark.unit
class TestSummarizeNewsWithGemini:
    """Test suite for summarize_news_with_gemini function."""

    @responses.activate
    @patch("dags.etl_modules.notifications.get_latest_stock_data")
    def test_successful_summary_generation(
        self, mock_get_data, mock_environment_variables
    ):
        """Test successful AI summary generation."""
        # Setup mocks with all required fields
        mock_get_data.return_value = {
            "HPG": {
                "close": 25500.0,
                "daily_return": 2.5,
                "return_1m": 8.5,
                "rsi_14": 45.2,
                "macd": 100.5,
                "macd_signal": 95.3,
                "macd_hist": 5.2,
                "ma_50": 25000.0,
                "ma_200": 24000.0,
                "pe_ratio": 15.5,
                "roe": 18.5,
                "roic": 12.5,
                "debt_to_equity": 0.5,
                "sector": "Steel",
                "industry": "Basic Materials",
            }
        }

        responses.add(
            responses.POST, get_gemini_url(), json=mock_gemini_success(), status=200
        )

        news_data = [
            {
                "ticker": "HPG",
                "title": "Test article",
                "price_change": 2.5,
                "publish_date": datetime(2024, 12, 20),
            }
        ]

        # Execute
        result = summarize_news_with_gemini(news_data)

        # Assert
        assert result is not None
        assert "Tổng quan thị trường" in result
        assert "HPG" in result

    @responses.activate
    @patch("dags.etl_modules.notifications.get_latest_stock_data")
    def test_api_error_returns_none(self, mock_get_data, mock_environment_variables):
        """Test that API errors return None."""
        mock_get_data.return_value = {}

        responses.add(
            responses.POST,
            get_gemini_url(),
            json={"error": "Invalid API key"},
            status=400,
        )

        news_data = [{"ticker": "HPG", "title": "Test"}]

        result = summarize_news_with_gemini(news_data)

        assert result is None

    def test_missing_api_key_returns_none(self, monkeypatch):
        """Test that missing API key returns None."""
        monkeypatch.delenv("GEMINI_API_KEY", raising=False)

        result = summarize_news_with_gemini([{"ticker": "HPG", "title": "Test"}])

        assert result is None
