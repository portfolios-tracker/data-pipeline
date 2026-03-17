"""
data-pipeline/tests/unit/test_price_adjuster.py

Unit tests for the backward chain-linked price adjustment algorithm (Story 2.1).
No ClickHouse connection -- all data is passed as in-memory DataFrames.

Algorithm under test (price_adjuster.calculate_adjusted_prices):
  - cash_factor   = (p - cash_pct * 100) / p     (p = price on ex-date, in VND)
  - split_factor  = 1.0 / (1.0 + stock_pct / 100)
  - combined_factor applied to all trading_date STRICTLY BEFORE ex_date
"""

from datetime import date

import pandas as pd
import pytest
from dags.etl_modules.price_adjuster import calculate_adjusted_prices

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

REQUIRED_COLUMNS = {
    "ticker",
    "trading_date",
    "adjusted_close",
    "adjusted_volume",
    "raw_close",
    "adj_factor",
}


def make_prices(rows: list[tuple]) -> pd.DataFrame:
    """Build a raw_prices DataFrame from (trading_date_str, close) tuples."""
    return pd.DataFrame(
        [(date.fromisoformat(d), float(c)) for d, c in rows],
        columns=["trading_date", "close"],
    )


def make_dividends(rows: list[tuple]) -> pd.DataFrame:
    """Build dividends from (exercise_date_str, cash_pct, stock_pct) tuples."""
    return pd.DataFrame(
        [(r[0], float(r[1]), float(r[2])) for r in rows],
        columns=[
            "exercise_date",
            "cash_dividend_percentage",
            "stock_dividend_percentage",
        ],
    )


def empty_dividends() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "exercise_date",
            "cash_dividend_percentage",
            "stock_dividend_percentage",
        ]
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestNoAdjustment:
    """Prices with no corporate actions should pass through unchanged."""

    def test_adjusted_equals_raw_with_no_events(self):
        prices = make_prices(
            [
                ("2024-01-02", 10000.0),
                ("2024-01-03", 10200.0),
                ("2024-01-04", 9800.0),
            ]
        )
        result = calculate_adjusted_prices(prices, empty_dividends(), "HPG")

        assert result["adjusted_close"].tolist() == pytest.approx(
            [10000.0, 10200.0, 9800.0]
        )
        assert result["raw_close"].tolist() == pytest.approx([10000.0, 10200.0, 9800.0])
        assert result["adj_factor"].tolist() == pytest.approx([1.0, 1.0, 1.0])

    def test_ticker_column_is_set(self):
        prices = make_prices([("2024-01-02", 5000.0)])
        result = calculate_adjusted_prices(prices, empty_dividends(), "VIC")
        assert result["ticker"].iloc[0] == "VIC"


class TestOutputSchema:
    """Output always has the required columns regardless of input."""

    def test_columns_with_data(self):
        prices = make_prices([("2024-01-02", 5000.0)])
        result = calculate_adjusted_prices(prices, empty_dividends(), "VNM")
        assert REQUIRED_COLUMNS == set(result.columns)

    def test_empty_input_returns_empty_with_correct_columns(self):
        empty = pd.DataFrame(columns=["trading_date", "close"])
        result = calculate_adjusted_prices(empty, empty_dividends(), "XXX")
        assert result.empty
        assert REQUIRED_COLUMNS == set(result.columns)


class TestCashDividend:
    """
    cash_factor = (p - cash_pct * 100) / p  (VN par-value convention).

    Example: p=10000 VND, cash_pct=10 -> dividend=1000 VND -> factor=0.9.
    """

    def test_prices_before_ex_date_adjusted(self):
        prices = make_prices(
            [
                ("2024-01-03", 9500.0),  # before  -> adjusted
                ("2024-01-04", 9800.0),  # before  -> adjusted
                ("2024-01-05", 10000.0),  # ex-date -> NOT adjusted (boundary is strict)
                ("2024-01-08", 9900.0),  # after   -> NOT adjusted
            ]
        )
        divs = make_dividends([("2024-01-05", 10.0, 0.0)])
        expected_factor = (10000.0 - 10.0 * 100.0) / 10000.0  # 0.9

        result = calculate_adjusted_prices(prices, divs, "VCB").set_index(
            "trading_date"
        )

        assert result.loc[date(2024, 1, 3), "adjusted_close"] == pytest.approx(
            9500.0 * expected_factor
        )
        assert result.loc[date(2024, 1, 4), "adjusted_close"] == pytest.approx(
            9800.0 * expected_factor
        )

    def test_price_on_ex_date_not_adjusted(self):
        prices = make_prices(
            [
                ("2024-01-04", 9800.0),
                ("2024-01-05", 10000.0),
            ]
        )
        result = calculate_adjusted_prices(
            prices, make_dividends([("2024-01-05", 10.0, 0.0)]), "VCB"
        ).set_index("trading_date")
        assert result.loc[date(2024, 1, 5), "adjusted_close"] == pytest.approx(10000.0)

    def test_price_after_ex_date_not_adjusted(self):
        prices = make_prices(
            [
                ("2024-01-05", 10000.0),
                ("2024-01-08", 9900.0),
            ]
        )
        result = calculate_adjusted_prices(
            prices, make_dividends([("2024-01-05", 10.0, 0.0)]), "VCB"
        ).set_index("trading_date")
        assert result.loc[date(2024, 1, 8), "adjusted_close"] == pytest.approx(9900.0)

    def test_oversized_dividend_guard_skips_event(self):
        """When dividend >= price the guard keeps factor=1 (no adjustment)."""
        prices = make_prices(
            [
                ("2024-01-04", 1000.0),  # before ex-date
                (
                    "2024-01-05",
                    1000.0,
                ),  # ex-date; dividend=50*100=5000 > 1000 -> skipped
            ]
        )
        result = calculate_adjusted_prices(
            prices, make_dividends([("2024-01-05", 50.0, 0.0)]), "XXX"
        ).set_index("trading_date")
        assert result.loc[date(2024, 1, 4), "adjusted_close"] == pytest.approx(1000.0)


class TestStockDividend:
    """
    split_factor = 1 / (1 + stock_pct / 100).

    Example: stock_pct=10 -> 10 bonus per 100 held -> 1 share becomes 1.1 -> factor=1/1.1.
    """

    def test_prices_before_ex_date_adjusted(self):
        prices = make_prices(
            [
                ("2024-03-01", 22000.0),
                ("2024-03-04", 23000.0),
                ("2024-03-05", 21000.0),  # ex-date
                ("2024-03-06", 20000.0),
            ]
        )
        divs = make_dividends([("2024-03-05", 0.0, 10.0)])
        expected_factor = 1.0 / 1.10

        result = calculate_adjusted_prices(prices, divs, "FPT").set_index(
            "trading_date"
        )

        assert result.loc[date(2024, 3, 1), "adjusted_close"] == pytest.approx(
            22000.0 * expected_factor, rel=1e-5
        )
        assert result.loc[date(2024, 3, 4), "adjusted_close"] == pytest.approx(
            23000.0 * expected_factor, rel=1e-5
        )

    def test_price_on_and_after_ex_date_unchanged(self):
        prices = make_prices(
            [
                ("2024-03-05", 21000.0),
                ("2024-03-06", 20000.0),
            ]
        )
        result = calculate_adjusted_prices(
            prices, make_dividends([("2024-03-05", 0.0, 10.0)]), "FPT"
        ).set_index("trading_date")
        assert result.loc[date(2024, 3, 5), "adjusted_close"] == pytest.approx(21000.0)
        assert result.loc[date(2024, 3, 6), "adjusted_close"] == pytest.approx(20000.0)


class TestMultipleEvents:
    """
    Multiple events compound correctly (backward order).

    Event A (2024-03-01): stock_pct=10 -> fa = 1/1.10
    Event B (2024-05-01): stock_pct=20 -> fb = 1/1.20

    Segments:
      before A      -> fa * fb
      between A & B -> fb only
      on/after B    -> unchanged
    """

    def setup_method(self):
        self.prices = make_prices(
            [
                ("2024-01-10", 10000.0),  # before both
                ("2024-02-10", 11000.0),  # before both
                ("2024-03-01", 12000.0),  # ex-date A
                ("2024-04-01", 13000.0),  # between A and B
                ("2024-05-01", 14000.0),  # ex-date B
                ("2024-06-01", 15000.0),  # after B
            ]
        )
        self.divs = make_dividends(
            [
                ("2024-03-01", 0.0, 10.0),
                ("2024-05-01", 0.0, 20.0),
            ]
        )
        self.result = calculate_adjusted_prices(
            self.prices, self.divs, "MWG"
        ).set_index("trading_date")
        self.fa = 1.0 / 1.10
        self.fb = 1.0 / 1.20

    def test_before_both_events_compounded(self):
        assert self.result.loc[date(2024, 1, 10), "adjusted_close"] == pytest.approx(
            10000.0 * self.fa * self.fb, rel=1e-5
        )
        assert self.result.loc[date(2024, 2, 10), "adjusted_close"] == pytest.approx(
            11000.0 * self.fa * self.fb, rel=1e-5
        )

    def test_between_events_only_later_factor(self):
        assert self.result.loc[date(2024, 4, 1), "adjusted_close"] == pytest.approx(
            13000.0 * self.fb, rel=1e-5
        )

    def test_on_ex_dates_not_adjusted_by_own_event(self):
        # ex-date A (2024-03-01) is NOT adjusted by its own Event A,
        # but IS adjusted by the later Event B because 2024-03-01 < 2024-05-01 (Event B ex-date).
        assert self.result.loc[date(2024, 3, 1), "adjusted_close"] == pytest.approx(
            12000.0 * self.fb, rel=1e-5
        )
        # ex-date B (2024-05-01) is the latest event — nothing adjusts it.
        assert self.result.loc[date(2024, 5, 1), "adjusted_close"] == pytest.approx(
            14000.0
        )

    def test_after_all_events_unchanged(self):
        assert self.result.loc[date(2024, 6, 1), "adjusted_close"] == pytest.approx(
            15000.0
        )

    def test_adj_factor_matches_compounded_factor(self):
        assert self.result.loc[date(2024, 1, 10), "adj_factor"] == pytest.approx(
            self.fa * self.fb, rel=1e-5
        )

    def test_raw_close_never_modified(self):
        originals = {
            date(2024, 1, 10): 10000.0,
            date(2024, 2, 10): 11000.0,
            date(2024, 3, 1): 12000.0,
            date(2024, 4, 1): 13000.0,
            date(2024, 5, 1): 14000.0,
            date(2024, 6, 1): 15000.0,
        }
        for dt, raw in originals.items():
            assert self.result.loc[dt, "raw_close"] == pytest.approx(raw)
