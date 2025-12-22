"""
Unit tests for dags/etl_modules/fetcher.py

Tests cover:
- clean_decimal_cols: Data cleaning for ClickHouse Decimal types
- fetch_stock_price: Stock price fetching with technical indicators
- fetch_financial_ratios: Financial ratios extraction
- fetch_income_stmt: Income statement data
- fetch_dividends: Dividend history
- fetch_news: News articles fetching
"""

from datetime import date, datetime
from unittest.mock import Mock, patch

import numpy as np
import pandas as pd
import pytest

from dags.etl_modules.fetcher import (
    clean_decimal_cols,
    fetch_dividends,
    fetch_financial_ratios,
    fetch_income_stmt,
    fetch_news,
    fetch_stock_price,
)

# ============================================================================
# Tests for clean_decimal_cols()
# ============================================================================


@pytest.mark.unit
class TestCleanDecimalCols:
    """Test suite for clean_decimal_cols helper function."""

    def test_replaces_nan_with_zero(self):
        """Test that NaN values are replaced with 0."""
        df = pd.DataFrame({"price": [100.5, np.nan, 200.0]})
        result = clean_decimal_cols(df, ["price"])

        assert result["price"].isna().sum() == 0
        assert result["price"][1] == 0.0

    def test_replaces_infinity_with_zero(self):
        """Test that Infinity values are replaced with 0."""
        df = pd.DataFrame({"value": [10.0, np.inf, -np.inf, 20.0]})
        result = clean_decimal_cols(df, ["value"])

        assert not np.isinf(result["value"]).any()
        assert result["value"][1] == 0.0
        assert result["value"][2] == 0.0

    def test_coerces_string_to_numeric(self):
        """Test that string values are coerced to numeric (becomes NaN then 0)."""
        df = pd.DataFrame({"amount": ["100", "invalid", "200"]})
        result = clean_decimal_cols(df, ["amount"])

        assert result["amount"][0] == 100.0
        assert result["amount"][1] == 0.0  # 'invalid' -> NaN -> 0
        assert result["amount"][2] == 200.0

    def test_handles_multiple_columns(self):
        """Test cleaning multiple columns simultaneously."""
        df = pd.DataFrame(
            {
                "col1": [1.0, np.nan, 3.0],
                "col2": [np.inf, 5.0, -np.inf],
                "col3": [7.0, 8.0, 9.0],  # This one stays untouched
            }
        )
        result = clean_decimal_cols(df, ["col1", "col2"])

        assert result["col1"][1] == 0.0
        assert result["col2"][0] == 0.0
        assert result["col2"][2] == 0.0
        assert result["col3"][0] == 7.0  # Unchanged

    def test_handles_missing_columns_gracefully(self):
        """Test that function doesn't fail if column doesn't exist."""
        df = pd.DataFrame({"price": [100.0, 200.0]})
        # Should not raise an error
        result = clean_decimal_cols(df, ["price", "nonexistent_col"])

        assert result["price"][0] == 100.0
        assert "nonexistent_col" not in result.columns

    def test_preserves_valid_values(self):
        """Test that valid numeric values are preserved."""
        df = pd.DataFrame({"value": [1.5, 2.7, 3.14159, 0.0, -5.2]})
        result = clean_decimal_cols(df, ["value"])

        assert result["value"].tolist() == [1.5, 2.7, 3.14159, 0.0, -5.2]

    def test_handles_none_values(self):
        """Test that None values are replaced with 0."""
        df = pd.DataFrame({"price": [100.0, None, 200.0]})
        result = clean_decimal_cols(df, ["price"])

        assert result["price"][1] == 0.0

    def test_empty_dataframe(self):
        """Test handling of empty DataFrame."""
        df = pd.DataFrame()
        result = clean_decimal_cols(df, ["price"])

        assert result.empty


# ============================================================================
# Tests for fetch_stock_price()
# ============================================================================


@pytest.mark.unit
class TestFetchStockPrice:
    """Test suite for fetch_stock_price function."""

    @patch("dags.etl_modules.fetcher.Quote")
    def test_successful_fetch_returns_dataframe(self, mock_quote_class):
        """Test successful stock price fetch returns properly formatted DataFrame."""
        # Setup mock
        mock_quote = Mock()
        mock_df = pd.DataFrame(
            {
                "time": pd.date_range("2024-01-01", periods=10),
                "open": [100.0] * 10,
                "high": [101.0] * 10,
                "low": [99.0] * 10,
                "close": [100.5] * 10,
                "volume": [1000000] * 10,
            }
        )
        mock_quote.history.return_value = mock_df
        mock_quote_class.return_value = mock_quote

        # Execute
        result = fetch_stock_price("HPG", "2024-01-01", "2024-01-10")

        # Assert
        assert not result.empty
        assert "ticker" in result.columns
        assert "trading_date" in result.columns
        assert result["ticker"].iloc[0] == "HPG"
        assert len(result) == 10

    @patch("dags.etl_modules.fetcher.Quote")
    def test_empty_response_returns_empty_dataframe(self, mock_quote_class):
        """Test that empty API response returns empty DataFrame."""
        mock_quote = Mock()
        mock_quote.history.return_value = pd.DataFrame()
        mock_quote_class.return_value = mock_quote

        result = fetch_stock_price("INVALID", "2024-01-01", "2024-01-10")

        assert result.empty

    @patch("dags.etl_modules.fetcher.Quote")
    def test_api_exception_returns_empty_dataframe(self, mock_quote_class):
        """Test that API exceptions are caught and return empty DataFrame."""
        mock_quote_class.side_effect = Exception("API Error")

        result = fetch_stock_price("HPG", "2024-01-01", "2024-01-10")

        assert result.empty

    @patch("dags.etl_modules.fetcher.Quote")
    def test_technical_indicators_calculated(self, mock_quote_class):
        """Test that technical indicators (MA, RSI, MACD) are calculated."""
        # Setup mock with enough data for indicators
        mock_quote = Mock()
        dates = pd.date_range("2024-01-01", periods=250)
        prices = [100 + i * 0.1 for i in range(250)]

        mock_df = pd.DataFrame(
            {
                "time": dates,
                "open": prices,
                "high": [p * 1.01 for p in prices],
                "low": [p * 0.99 for p in prices],
                "close": prices,
                "volume": [1000000] * 250,
            }
        )
        mock_quote.history.return_value = mock_df
        mock_quote_class.return_value = mock_quote

        result = fetch_stock_price("HPG", "2024-01-01", "2024-12-31")

        # Check indicators exist
        assert "ma_50" in result.columns
        assert "ma_200" in result.columns
        assert "rsi_14" in result.columns
        assert "macd" in result.columns
        assert "macd_signal" in result.columns
        assert "macd_hist" in result.columns
        assert "daily_return" in result.columns

    @patch("dags.etl_modules.fetcher.Quote")
    def test_nan_values_cleaned(self, mock_quote_class):
        """Test that NaN values in prices are cleaned to 0."""
        mock_quote = Mock()
        mock_df = pd.DataFrame(
            {
                "time": pd.date_range("2024-01-01", periods=10),
                "open": [100.0, np.nan, 102.0] + [100.0] * 7,
                "high": [101.0] * 10,
                "low": [99.0] * 10,
                "close": [100.5] * 10,
                "volume": [1000000] * 10,
            }
        )
        mock_quote.history.return_value = mock_df
        mock_quote_class.return_value = mock_quote

        result = fetch_stock_price("HPG", "2024-01-01", "2024-01-10")

        assert result["open"].isna().sum() == 0
        assert result["open"].iloc[1] == 0.0

    @patch("dags.etl_modules.fetcher.Quote")
    def test_trading_date_converted_to_date(self, mock_quote_class):
        """Test that trading_date is converted to date type (not datetime)."""
        mock_quote = Mock()
        mock_df = pd.DataFrame(
            {
                "time": pd.date_range("2024-01-01", periods=5),
                "open": [100.0] * 5,
                "high": [101.0] * 5,
                "low": [99.0] * 5,
                "close": [100.5] * 5,
                "volume": [1000000] * 5,
            }
        )
        mock_quote.history.return_value = mock_df
        mock_quote_class.return_value = mock_quote

        result = fetch_stock_price("HPG", "2024-01-01", "2024-01-05")

        # Check that trading_date is date type, not datetime
        assert isinstance(result["trading_date"].iloc[0], date)
        assert not isinstance(result["trading_date"].iloc[0], datetime)


# ============================================================================
# Tests for fetch_financial_ratios()
# ============================================================================


@pytest.mark.unit
class TestFetchFinancialRatios:
    """Test suite for fetch_financial_ratios function."""

    @patch("dags.etl_modules.fetcher.Finance")
    def test_successful_fetch_returns_dataframe(self, mock_finance_class):
        """Test successful ratio fetch returns properly formatted DataFrame."""
        mock_finance = Mock()

        # Create MultiIndex DataFrame like vnstock returns (with yearReport/lengthReport)
        data = {
            ("Chỉ số định giá", "P/E"): [15.5, 16.2],
            ("Chỉ số định giá", "P/B"): [2.1, 2.3],
            ("Chỉ số định giá", "P/S"): [1.5, 1.6],
            ("Chỉ số hiệu quả hoạt động", "ROE (%)"): [18.5, 19.2],
            ("Chỉ số tăng trưởng", "EPS"): [5000, 5200],
            ("Năm báo cáo", "yearReport"): [2024, 2024],
            ("Độ dài báo cáo", "lengthReport"): [4, 3],
        }
        mock_df = pd.DataFrame(data)
        mock_df.columns = pd.MultiIndex.from_tuples(mock_df.columns)

        mock_finance.ratio.return_value = mock_df
        mock_finance_class.return_value = mock_finance

        result = fetch_financial_ratios("HPG")

        assert not result.empty
        assert "ticker" in result.columns
        assert "fiscal_date" in result.columns
        assert result["ticker"].iloc[0] == "HPG"

    @patch("dags.etl_modules.fetcher.Finance")
    def test_empty_response_returns_empty_dataframe(self, mock_finance_class):
        """Test that empty response returns empty DataFrame."""
        mock_finance = Mock()
        mock_finance.ratio.return_value = pd.DataFrame()
        mock_finance_class.return_value = mock_finance

        result = fetch_financial_ratios("INVALID")

        assert result.empty

    @patch("dags.etl_modules.fetcher.Finance")
    def test_column_mapping_applied(self, mock_finance_class):
        """Test that Vietnamese column names are mapped to English."""
        mock_finance = Mock()

        data = {
            ("Chỉ số định giá", "P/E"): [15.5],
            ("Chỉ số hiệu quả hoạt động", "ROE (%)"): [18.5],
            ("Năm báo cáo", "yearReport"): [2024],
            ("Độ dài báo cáo", "lengthReport"): [4],
        }
        mock_df = pd.DataFrame(data)
        mock_df.columns = pd.MultiIndex.from_tuples(mock_df.columns)

        mock_finance.ratio.return_value = mock_df
        mock_finance_class.return_value = mock_finance

        result = fetch_financial_ratios("HPG")

        assert "pe_ratio" in result.columns
        assert "roe" in result.columns


# ============================================================================
# Placeholder tests for other functions (to be expanded)
# ============================================================================


@pytest.mark.unit
def test_fetch_income_stmt_placeholder():
    """Placeholder test for fetch_income_stmt - to be implemented."""
    # Placeholder removed in Phase 2
    assert True


@pytest.mark.unit
def test_fetch_dividends_placeholder():
    """Placeholder test for fetch_dividends - to be implemented."""
    # Placeholder removed in Phase 2
    assert True


@pytest.mark.unit
def test_fetch_news_placeholder():
    """Placeholder test for fetch_news - to be implemented."""
    # Placeholder removed in Phase 2
    assert True


# ============================================================================
# Phase 2: Tests for fetch_income_stmt(), fetch_dividends(), fetch_news()
# ============================================================================


@pytest.mark.unit
class TestFetchIncomeStmt:
    """Unit tests for fetch_income_stmt function."""

    @patch("dags.etl_modules.fetcher.Finance")
    def test_income_stmt_success(self, mock_finance_class):
        mock_finance = Mock()
        # Mock VCI income statement with expected English column names and year/quarter metadata
        df = pd.DataFrame(
            {
                "Net Sales": [5_000_000_000_000],
                "Cost of Sales": [3_500_000_000_000],
                "Gross Profit": [1_500_000_000_000],
                "Operating Profit/Loss": [800_000_000_000],
                "Net Profit For the Year": [600_000_000_000],
                "yearReport": [2024],
                "lengthReport": [4],
            }
        )
        mock_finance.income_statement.return_value = df
        mock_finance_class.return_value = mock_finance

        result = fetch_income_stmt("HPG")

        assert not result.empty
        assert set(
            [
                "ticker",
                "fiscal_date",
                "year",
                "quarter",
                "revenue",
                "cost_of_goods_sold",
                "gross_profit",
                "operating_profit",
                "net_profit_post_tax",
            ]
        ).issubset(result.columns)
        assert result["ticker"].iloc[0] == "HPG"
        assert result["fiscal_date"].iloc[0] == "2024-12-31"
        # Values preserved and cleaned
        assert result["revenue"].iloc[0] == 5_000_000_000_000

    @patch("dags.etl_modules.fetcher.Finance")
    def test_income_stmt_handles_missing_columns(self, mock_finance_class):
        mock_finance = Mock()
        # Missing some metrics, should fill with 0 and still return
        df = pd.DataFrame(
            {
                "Net Sales": [5_000_000_000_000],
                "yearReport": [2024],
                "lengthReport": [4],
            }
        )
        mock_finance.income_statement.return_value = df
        mock_finance_class.return_value = mock_finance

        result = fetch_income_stmt("HPG")

        assert not result.empty
        assert result["operating_profit"].iloc[0] == 0.0
        assert result["net_profit_post_tax"].iloc[0] == 0.0

    @patch("dags.etl_modules.fetcher.Finance")
    def test_income_stmt_empty_dataframe(self, mock_finance_class):
        mock_finance = Mock()
        mock_finance.income_statement.return_value = pd.DataFrame()
        mock_finance_class.return_value = mock_finance

        result = fetch_income_stmt("HPG")
        assert result.empty


@pytest.mark.unit
class TestFetchDividends:
    """Unit tests for fetch_dividends function."""

    @patch("dags.etl_modules.fetcher.Company")
    def test_dividends_success(self, mock_company_class):
        mock_company = Mock()
        df = pd.DataFrame(
            {
                "exercise_date": ["2024-06-15"],
                "cash_year": [2024],
                "cash_dividend_percentage": [15.0],
                "stock_dividend_percentage": [0.0],
                "issue_method": ["cash"],
            }
        )
        mock_company.dividends.return_value = df
        mock_company_class.return_value = mock_company

        result = fetch_dividends("HPG")

        assert not result.empty
        assert set(
            [
                "ticker",
                "exercise_date",
                "cash_year",
                "cash_dividend_percentage",
                "stock_dividend_percentage",
                "issue_method",
            ]
        ).issubset(result.columns)
        assert result["ticker"].iloc[0] == "HPG"
        assert str(result["exercise_date"].iloc[0]) == "2024-06-15"

    @patch("dags.etl_modules.fetcher.Company")
    def test_dividends_missing_fields_filled(self, mock_company_class):
        mock_company = Mock()
        df = pd.DataFrame(
            {
                "exercise_date": ["2024-06-15"],
            }
        )
        mock_company.dividends.return_value = df
        mock_company_class.return_value = mock_company

        result = fetch_dividends("HPG")
        assert not result.empty
        assert result["cash_year"].iloc[0] == 0
        assert result["cash_dividend_percentage"].iloc[0] == 0.0
        assert result["issue_method"].iloc[0] is None

    @patch("dags.etl_modules.fetcher.Company")
    def test_dividends_empty_dataframe(self, mock_company_class):
        mock_company = Mock()
        mock_company.dividends.return_value = pd.DataFrame()
        mock_company_class.return_value = mock_company

        result = fetch_dividends("HPG")
        assert result.empty


@pytest.mark.unit
class TestFetchNews:
    """Unit tests for fetch_news function."""

    @patch("dags.etl_modules.fetcher.Company")
    def test_news_success(self, mock_company_class):
        mock_company = Mock()
        df = pd.DataFrame(
            {
                "publish_date": ["2024-12-20T10:30:00"],
                "title": ["HPG announces strong Q4 results"],
                "source": ["CafeF"],
                "price": [25500],
                "price_change": [2.5],
                "price_change_ratio": [0.012],
                "rsi": [45.2],
                "rs": [0.5],
                "id": [12345],
            }
        )
        mock_company.news.return_value = df
        mock_company_class.return_value = mock_company

        result = fetch_news("HPG")

        assert not result.empty
        assert set(
            [
                "ticker",
                "publish_date",
                "title",
                "source",
                "price_at_publish",
                "price_change",
                "price_change_ratio",
                "rsi",
                "rs",
                "news_id",
            ]
        ).issubset(result.columns)
        assert result["ticker"].iloc[0] == "HPG"
        assert (
            pd.to_datetime(result["publish_date"].iloc[0]).strftime("%Y-%m-%d")
            == "2024-12-20"
        )
        assert result["price_at_publish"].iloc[0] == 25500

    @patch("dags.etl_modules.fetcher.Company")
    def test_news_missing_fields_filled(self, mock_company_class):
        mock_company = Mock()
        df = pd.DataFrame(
            {
                "publish_date": ["2024-12-20T10:30:00"],
                "title": ["HPG update"],
                "source": ["CafeF"],
            }
        )
        mock_company.news.return_value = df
        mock_company_class.return_value = mock_company

        result = fetch_news("HPG")
        assert not result.empty
        assert result["price_at_publish"].iloc[0] == 0.0
        # Default for non-price/rs fields is None per implementation
        assert result["news_id"].iloc[0] is None

    @patch("dags.etl_modules.fetcher.Company")
    def test_news_empty_dataframe(self, mock_company_class):
        mock_company = Mock()
        mock_company.news.return_value = pd.DataFrame()
        mock_company_class.return_value = mock_company

        result = fetch_news("HPG")
        assert result.empty
