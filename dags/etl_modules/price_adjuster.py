"""
services/data-pipeline/dags/etl_modules/price_adjuster.py

Backward chain-linked price adjustment algorithm for VN market stocks.

Algorithm (Story 2.1):
  For each corporate action event sorted in DESCENDING date order:
    1. Calculate adjustment factor from the price on the ex-date.
    2. Multiply all STRICTLY PRIOR closing prices by that factor.

This gives "Total Return" backward-adjusted prices: the ratio between any two
dates' adjusted prices equals the actual total return for that period.

VN Market conventions:
  - Cash dividends stored as % of 10,000 VND par value
    (e.g., cash_dividend_percentage=30 → 3,000 VND/share)
  - Stock dividends stored as % of shares held
    (e.g., stock_dividend_percentage=10 → 10 extra shares per 100 held, i.e. ×1.10 split)

No ClickHouse connection required in this module — data is passed as DataFrames
so that unit tests can run without any infrastructure.
"""

import logging

import pandas as pd

logger = logging.getLogger(__name__)


def calculate_adjusted_prices(
    raw_prices: pd.DataFrame,
    dividends: pd.DataFrame,
    ticker: str,
) -> pd.DataFrame:
    """
    Apply backward chain-linked adjustment to a single ticker's price series.

    Parameters
    ----------
    raw_prices : pd.DataFrame
        Must contain columns: ``trading_date`` (date), ``close`` (numeric).
        Optional column: ``volume``. The builder consumes a close-based series;
        TradingView handles detailed asset charting elsewhere in the product.
    dividends : pd.DataFrame
        Must contain columns: ``exercise_date`` (date-like),
        ``cash_dividend_percentage`` (float), ``stock_dividend_percentage`` (float).
        Missing or None values in percentage columns are treated as 0.
    ticker : str
        Ticker symbol — added to the output rows.

    Returns
    -------
    pd.DataFrame
        Columns: ticker, trading_date, adjusted_close, adjusted_volume,
                 raw_close, adj_factor
    """
    if raw_prices.empty:
        logger.warning("calculate_adjusted_prices: empty raw_prices for %s", ticker)
        return pd.DataFrame(
            columns=[
                "ticker", "trading_date",
                "adjusted_close", "adjusted_volume", "raw_close", "adj_factor",
            ]
        )

    df = raw_prices[[c for c in ["trading_date", "close", "volume"] if c in raw_prices.columns]].copy()
    df["trading_date"] = pd.to_datetime(df["trading_date"]).dt.date
    df = df.sort_values("trading_date").reset_index(drop=True)

    # Adjusted close starts equal to raw close.
    df["adjusted_close"] = df["close"].astype(float)
    # raw_close preserved without modification throughout the algorithm
    df["raw_close"] = df["close"].astype(float)
    df["cumulative_factor"] = 1.0
    # adjusted_volume initialised from source; inverse-adjusted for stock splits only
    df["adjusted_volume"] = df["volume"].astype(float) if "volume" in df.columns else 0.0

    if dividends.empty:
        df["ticker"] = ticker
        df["adj_factor"] = df["cumulative_factor"]
        return df[[
            "ticker", "trading_date",
            "adjusted_close", "adjusted_volume", "raw_close", "adj_factor",
        ]]

    # Sort corporate actions in DESCENDING order: most recent first.
    # Backward adjustment must process events from newest to oldest so that
    # each factor is applied only to prices BEFORE that event.
    divs = dividends.copy()
    divs["exercise_date"] = pd.to_datetime(divs["exercise_date"]).dt.date
    divs = divs.sort_values("exercise_date", ascending=False).reset_index(drop=True)

    for _, div in divs.iterrows():
        ex_date = div["exercise_date"]

        cash_pct = float(div.get("cash_dividend_percentage") or 0)
        stock_pct = float(div.get("stock_dividend_percentage") or 0)

        if cash_pct == 0 and stock_pct == 0:
            continue  # No adjustment needed

        # Determine price ON the ex-date (use the post-event adjusted price for chaining)
        price_mask = df["trading_date"] == ex_date
        price_on_ex = df.loc[price_mask, "adjusted_close"]
        if price_on_ex.empty:
            logger.debug(
                "No price data on ex-date %s for %s — skipping this event", ex_date, ticker
            )
            continue

        p = float(price_on_ex.iloc[0])
        if p <= 0:
            logger.warning("Zero or negative price on ex-date %s for %s — skipping", ex_date, ticker)
            continue

        # --- Cash dividend factor ---
        # VN: cash_dividend_percentage is % of 10,000 VND par value.
        # Example: 30% → 30 × 100 = 3,000 VND per share.
        dividend_per_share = cash_pct * 100.0
        cash_factor = (p - dividend_per_share) / p if p > dividend_per_share else 1.0

        # --- Stock split / stock dividend factor ---
        # VN: 10% stock dividend → each share becomes 1.10 shares → price factor = 1/1.10
        split_factor = 1.0 / (1.0 + stock_pct / 100.0) if stock_pct > 0 else 1.0

        combined_factor = cash_factor * split_factor
        if combined_factor <= 0:
            logger.warning(
                "Combined factor %.6f <= 0 for %s on %s — skipping", combined_factor, ticker, ex_date
            )
            continue

        # Apply to all dates STRICTLY BEFORE ex_date (backward adjustment)
        prior_mask = df["trading_date"] < ex_date
        df.loc[prior_mask, "adjusted_close"] *= combined_factor
        df.loc[prior_mask, "cumulative_factor"] *= combined_factor

        # Volume scales inversely: more shares outstanding after split
        # e.g., 10% stock div → historical volume × 1.10 (only for splits; cash divs don't affect volume)
        if stock_pct > 0:
            volume_factor = 1.0 + stock_pct / 100.0  # = 1/split_factor
            df.loc[prior_mask, "adjusted_volume"] *= volume_factor

    df["ticker"] = ticker
    df["adj_factor"] = df["cumulative_factor"]
    return df[[
        "ticker", "trading_date",
        "adjusted_close", "adjusted_volume", "raw_close", "adj_factor",
    ]]
