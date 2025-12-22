import pandas as pd
import pandas_ta as ta
import logging
import numpy as np
from vnstock import Quote, Finance, Company
from urllib.parse import urlparse


def clean_decimal_cols(df, cols):
    """
    Helper to robustly clean columns destined for ClickHouse Decimal types.
    Replaces NaN, None, and Infinity with 0.
    """
    for col in cols:
        if col in df.columns:
            # 1. Coerce to numeric (turns strings/garbage into NaN)
            df[col] = pd.to_numeric(df[col], errors="coerce")
            # 2. Replace Infinity with NaN (so we can fillna them next)
            df[col] = df[col].replace([np.inf, -np.inf], np.nan)
            # 3. Fill NaN with 0
            df[col] = df[col].fillna(0)
    return df


def fetch_stock_price(symbol, start_date, end_date):
    logging.info(f"Attempting fetch for {symbol}...")
    df = pd.DataFrame()
    try:
        quote = Quote(symbol=symbol, source="vci")
        df = quote.history(start=start_date, end=end_date, interval="D")
        if df is None or df.empty:
            raise ValueError("Empty data")

        df.columns = [c.lower() for c in df.columns]
        df.rename(
            columns={"time": "trading_date", "date": "trading_date"}, inplace=True
        )

        required_cols = ["trading_date", "open", "high", "low", "close", "volume"]
        df = df[[c for c in required_cols if c in df.columns]]
        df["ticker"] = symbol
        df["source"] = "vnstock"
    except Exception as e:
        logging.warning(f"VNSTOCK failed for {symbol}: {e}")
        return pd.DataFrame()

    if df.empty:
        return df

    # Type Conversion
    if not pd.api.types.is_datetime64_any_dtype(df["trading_date"]):
        df["trading_date"] = pd.to_datetime(df["trading_date"])
    df["trading_date"] = df["trading_date"].dt.date

    # Clean for Decimal
    df = clean_decimal_cols(df, ["open", "high", "low", "close"])
    df["volume"] = df["volume"].fillna(0).astype(int)

    # Technical Indicators
    try:
        df["calc_date"] = pd.to_datetime(df["trading_date"])
        df.set_index("calc_date", inplace=True)
        df.sort_index(inplace=True)

        df["ma_50"] = ta.sma(df["close"], length=50)
        df["ma_200"] = ta.sma(df["close"], length=200)
        df["rsi_14"] = ta.rsi(df["close"], length=14)
        df["daily_return"] = df["close"].pct_change() * 100

        # MACD (New)
        macd = ta.macd(df["close"], fast=12, slow=26, signal=9)
        if macd is not None:
            # pandas_ta returns columns like MACD_12_26_9, MACDh_..., MACDs_...
            # We rename them to our simple schema
            df["macd"] = macd.iloc[:, 0]  # MACD Line
            df["macd_hist"] = macd.iloc[:, 1]  # Histogram
            df["macd_signal"] = macd.iloc[:, 2]  # Signal Line

        # Backfill & FillNa
        cols_to_fill = [
            "ma_50",
            "ma_200",
            "rsi_14",
            "daily_return",
            "macd",
            "macd_signal",
            "macd_hist",
        ]
        for c in cols_to_fill:
            if c in df.columns:
                df[c] = df[c].bfill().fillna(0)
            else:
                df[c] = 0.0

        df.reset_index(drop=True, inplace=True)
    except Exception as e:
        logging.error(f"Error indicators {symbol}: {e}")
        for c in [
            "ma_50",
            "ma_200",
            "rsi_14",
            "daily_return",
            "macd",
            "macd_signal",
            "macd_hist",
        ]:
            df[c] = 0.0

    return df


def fetch_financial_ratios(symbol):
    logging.info(f"Fetching ratios for {symbol}...")
    try:
        finance = Finance(symbol=symbol, source="VCI")
        df = finance.ratio(period="quarter", lang="en", dropna=True)
        if df is None or df.empty:
            return pd.DataFrame()

        # 1. Flatten MultiIndex Columns
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = ["_".join(map(str, col)).strip() for col in df.columns.values]

        col_list = df.columns.tolist()

        def get_col(keyword):
            for c in col_list:
                if keyword in c:
                    return c
            return None

        # 2. Map Columns - Start with the source dataframe
        year_col = get_col("yearReport")
        quarter_col = get_col("lengthReport")
        if not year_col or not quarter_col:
            return pd.DataFrame()

        out_df = pd.DataFrame()
        out_df["year"] = df[year_col].fillna(0).astype(int)
        out_df["quarter"] = df[quarter_col].fillna(0).astype(int)

        # Mapping Dictionary: {Target: Source_Keyword}
        metric_map = {
            "pe_ratio": "P/E",
            "pb_ratio": "P/B",
            "ps_ratio": "P/S",
            "p_cashflow_ratio": "P/Cash Flow",
            "eps": "EPS",
            "bvps": "BVPS",
            "market_cap": "Market Capital",
            "roe": "ROE",
            "roa": "ROA",
            "roic": "ROIC",
            "net_profit_margin": "Net Profit Margin",
            "debt_to_equity": "Debt/Equity",
            "financial_leverage": "Financial Leverage",
            "dividend_yield": "Dividend yield",
        }

        for target, keyword in metric_map.items():
            src_col = get_col(keyword)
            out_df[target] = df[src_col] if src_col else 0.0

        # 3. Generate Date
        def get_quarter_end(row):
            y = int(row["year"])  # Ensure year is int
            q = int(row["quarter"])  # Ensure quarter is int
            if q == 1:
                return pd.Timestamp(f"{y}-03-31").date()
            if q == 2:
                return pd.Timestamp(f"{y}-06-30").date()
            if q == 3:
                return pd.Timestamp(f"{y}-09-30").date()
            if q == 4:
                return pd.Timestamp(f"{y}-12-31").date()
            return pd.Timestamp(f"{y}-01-01").date()

        out_df["fiscal_date"] = out_df.apply(get_quarter_end, axis=1)
        out_df = clean_decimal_cols(out_df, list(metric_map.keys()))

        # Add ticker column AFTER all other columns are set to ensure proper alignment
        out_df["ticker"] = symbol

        return out_df

    except Exception as e:
        logging.error(f"Error ratios {symbol}: {e}")
        return pd.DataFrame()


def fetch_income_stmt(symbol):
    """
    Fetches income statement.
    """
    try:
        finance = Finance(symbol=symbol, source="VCI")
        try:
            df = finance.income_statement(period="quarter", lang="en", dropna=True)
        except AttributeError:
            return pd.DataFrame()

        if df is None or df.empty:
            return pd.DataFrame()

        # Mapping
        mapping = {
            "Net Sales": "revenue",
            "Cost of Sales": "cost_of_goods_sold",
            "Gross Profit": "gross_profit",
            "Operating Profit/Loss": "operating_profit",
            "Net Profit For the Year": "net_profit_post_tax",
        }
        df.rename(columns=mapping, inplace=True)

        required_metrics = list(mapping.values())
        df_final = df.copy()

        # Ensure ticker
        if "ticker" not in df_final.columns:
            df_final["ticker"] = symbol

        # Handle Date
        if "yearReport" in df_final.columns and "lengthReport" in df_final.columns:
            df_final["year"] = df_final["yearReport"]
            df_final["quarter"] = df_final["lengthReport"]

            def make_date(row):
                try:
                    y = int(row["year"])
                    q = int(row["quarter"])
                    if q == 1:
                        return f"{y}-03-31"
                    if q == 2:
                        return f"{y}-06-30"
                    if q == 3:
                        return f"{y}-09-30"
                    if q == 4:
                        return f"{y}-12-31"
                except Exception as e:
                    logging.error(f"Error making date for {symbol}: {e}", exc_info=True)
                    pass
                return None

            df_final["fiscal_date"] = df_final.apply(make_date, axis=1)

        df_final.dropna(subset=["year", "quarter", "fiscal_date"], inplace=True)

        # Ensure columns exist and fill with 0 BEFORE type conversion
        for col in required_metrics:
            if col not in df_final.columns:
                df_final[col] = 0.0

        # Clean Decimal Columns
        df_final = clean_decimal_cols(df_final, required_metrics)

        # Select Final Columns
        final_cols = ["ticker", "fiscal_date", "year", "quarter"] + required_metrics
        return df_final[final_cols]

    except Exception as e:
        logging.error(f"Error fetching income stmt for {symbol}: {e}", exc_info=True)
        return pd.DataFrame()


def fetch_dividends(symbol):
    try:
        company = Company(symbol=symbol, source="TCBS")
        df = company.dividends()
        if df is None or df.empty:
            return pd.DataFrame()

        df["ticker"] = symbol
        if "exercise_date" in df.columns:
            df["exercise_date"] = pd.to_datetime(df["exercise_date"]).dt.date

        required_cols = [
            "ticker",
            "exercise_date",
            "cash_year",
            "cash_dividend_percentage",
            "stock_dividend_percentage",
            "issue_method",
        ]
        for col in required_cols:
            if col not in df.columns:
                df[col] = 0 if "percentage" in col or "year" in col else None

        df = clean_decimal_cols(
            df, ["cash_dividend_percentage", "stock_dividend_percentage"]
        )
        return df[required_cols]
    except Exception as e:
        logging.error(f"Error fetching dividends for {symbol}: {e}", exc_info=True)
        return pd.DataFrame()


def fetch_news(symbol):
    try:
        # TCBS public API has been deprecated; switch to VCI
        company = Company(symbol=symbol, source="VCI")
        df = company.news(page_size=50)
        if df is None or df.empty:
            return pd.DataFrame()

        df["ticker"] = symbol

        # Normalize column names between providers
        if "public_date" in df.columns:
            df.rename(columns={"public_date": "publish_date"}, inplace=True)
        if "news_title" in df.columns:
            df.rename(columns={"news_title": "title"}, inplace=True)

        # Only rename id -> news_id if news_id doesn't already exist
        if "id" in df.columns and "news_id" not in df.columns:
            df.rename(columns={"id": "news_id"}, inplace=True)

        # Map price columns
        if "price" in df.columns and "price_at_publish" not in df.columns:
            df.rename(columns={"price": "price_at_publish"}, inplace=True)
        elif "close_price" in df.columns:
            df["price_at_publish"] = pd.to_numeric(df["close_price"], errors="coerce")

        # Price change metrics
        if "ref_price" in df.columns and "close_price" in df.columns:
            df["price_change"] = pd.to_numeric(df["close_price"], errors="coerce") - pd.to_numeric(
                df["ref_price"], errors="coerce"
            )
        if "price_change_pct" in df.columns:
            df["price_change_ratio"] = pd.to_numeric(df["price_change_pct"], errors="coerce")

        # Derive source from link if available
        if "source" not in df.columns and "news_source_link" in df.columns:
            try:
                df["source"] = df["news_source_link"].apply(
                    lambda x: (urlparse(x).netloc if isinstance(x, str) else None)
                )
            except Exception:
                df["source"] = None

        # Publish date to datetime (VCI returns epoch ms)
        if "publish_date" in df.columns:
            try:
                if pd.api.types.is_numeric_dtype(df["publish_date"]):
                    df["publish_date"] = pd.to_datetime(df["publish_date"], unit="ms", errors="coerce")
                else:
                    df["publish_date"] = pd.to_datetime(df["publish_date"], errors="coerce")
            except Exception:
                df["publish_date"] = pd.to_datetime(df["publish_date"], errors="coerce")

        required_cols = [
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
        for col in required_cols:
            if col not in df.columns:
                df[col] = 0 if "price" in col or "rs" in col else None

        df = clean_decimal_cols(
            df, ["price_at_publish", "price_change", "price_change_ratio", "rsi", "rs"]
        )
        return df[required_cols]
    except Exception as e:
        logging.error(f"Error fetching news for {symbol}: {e}", exc_info=True)
        return pd.DataFrame()
