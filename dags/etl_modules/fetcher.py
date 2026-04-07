import logging
import os
from urllib.parse import urlparse

import numpy as np
import pandas as pd

try:
    from etl_modules.vci_provider import (
        fetch_balance_sheet as fetch_balance_sheet_frame,
    )
    from etl_modules.vci_provider import (
        fetch_company_events,
        fetch_company_news,
    )
    from etl_modules.vci_provider import (
        fetch_financial_ratios as fetch_financial_ratio_frame,
    )
    from etl_modules.vci_provider import (
        fetch_income_statement as fetch_income_statement_frame,
    )
    from etl_modules.vci_provider import (
        fetch_stock_price as fetch_stock_price_frame,
    )
    from etl_modules.vci_provider import (
        list_active_vn_stock_tickers as list_active_vn_stock_tickers_frame,
    )
except ModuleNotFoundError as exc:
    if exc.name != "etl_modules":
        raise
    from dags.etl_modules.vci_provider import (
        fetch_balance_sheet as fetch_balance_sheet_frame,
    )
    from dags.etl_modules.vci_provider import (
        fetch_company_events,
        fetch_company_news,
    )
    from dags.etl_modules.vci_provider import (
        fetch_financial_ratios as fetch_financial_ratio_frame,
    )
    from dags.etl_modules.vci_provider import (
        fetch_income_statement as fetch_income_statement_frame,
    )
    from dags.etl_modules.vci_provider import (
        fetch_stock_price as fetch_stock_price_frame,
    )
    from dags.etl_modules.vci_provider import (
        list_active_vn_stock_tickers as list_active_vn_stock_tickers_frame,
    )

try:
    from etl_modules.cache import cached_data
except ModuleNotFoundError as exc:
    if exc.name != "etl_modules":
        raise
    from dags.etl_modules.cache import cached_data

Company = None

# ---------------------------------------------------------------------------
# Fallback ticker list used when Supabase is unreachable during DAG parsing
# ---------------------------------------------------------------------------
_FALLBACK_VN_TICKERS = ["HPG", "VCB", "VNM", "FPT", "MWG", "VIC"]


def _extract_source_host(url: object) -> str | None:
    if not isinstance(url, str) or not url.strip():
        return None
    host = urlparse(url.strip()).netloc
    return host or None


# Temporary guardrail intentionally disabled.
# Keep this heuristic for quick rollback if provider metadata regresses.
#
# def _looks_like_non_equity_vn_symbol(symbol: str) -> bool:
#     symbol = str(symbol or "").strip().upper()
#     if not symbol:
#         return False
#     if re.match(r"^\d+[A-Z]\d+G\d+$", symbol):
#         return True
#     if re.match(r"^C[A-Z]{3,}\d{2,}$", symbol):
#         return True
#     return len(symbol) > 5 and any(ch.isdigit() for ch in symbol)


def get_active_vn_stock_tickers(
    raise_on_fallback: bool = False,
) -> list[dict[str, str]]:
    db_url = os.getenv("SUPABASE_DB_URL")
    assets = list_active_vn_stock_tickers_frame(
        db_url,
        raise_on_fallback=raise_on_fallback,
    )

    tickers = []
    seen_symbols = set()
    for row in assets:
        if not isinstance(row, dict):
            continue
        symbol = row.get("symbol")
        if not symbol:
            continue

        cleaned = str(symbol).strip().upper()
        metadata = row.get("metadata") or {}
        symbol_type = str(metadata.get("symbol_type") or "").strip().upper()
        if symbol_type and symbol_type != "STOCK":
            continue
        if cleaned and cleaned not in seen_symbols:
            asset_id = row.get("asset_id") or row.get("id") or "fallback"
            tickers.append({"symbol": cleaned, "asset_id": str(asset_id)})
            seen_symbols.add(cleaned)

    if tickers:
        return tickers

    if raise_on_fallback:
        raise RuntimeError(
            "market_data.assets query returned zero active VN stock tickers for market=VN and asset_class=STOCK"
        )
    return [{"symbol": t, "asset_id": "fallback"} for t in _FALLBACK_VN_TICKERS]


def get_active_vn_tickers(raise_on_fallback: bool = False) -> list[dict[str, str]]:
    """Backward-compatible alias for get_active_vn_stock_tickers()."""
    return get_active_vn_stock_tickers(raise_on_fallback=raise_on_fallback)


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
            # 3. Fill NaN with 0 and infer objects to avoid downcasting warning
            df[col] = df[col].fillna(0).infer_objects(copy=False)
    return df


@cached_data(ttl_seconds=43200)  # 12 hours
def fetch_stock_price(symbol, asset_id, start_date, end_date):
    logging.info(f"Attempting fetch for {symbol}...")
    try:
        df = fetch_stock_price_frame(symbol, start_date, end_date)
        if df is None or df.empty:
            raise ValueError("Empty data")

        df.columns = [c.lower() for c in df.columns]
        df.rename(
            columns={"time": "trading_date", "date": "trading_date"}, inplace=True
        )

        df["ticker"] = symbol
        df["asset_id"] = asset_id
        df["source"] = "vci"
    except Exception as e:
        logging.warning(f"VCI failed for {symbol}: {e}")
        return pd.DataFrame()

    if df.empty:
        return df

    # Type Conversion
    if not pd.api.types.is_datetime64_any_dtype(df["trading_date"]):
        df["trading_date"] = pd.to_datetime(df["trading_date"])
    df["trading_date"] = df["trading_date"].dt.date

    # Clean for Decimal
    df = clean_decimal_cols(df, ["close"])
    df["volume"] = df["volume"].fillna(0).astype(int)

    return df


@cached_data(ttl_seconds=86400)  # 24 hours
def fetch_financial_ratios(symbol, asset_id):
    logging.info(f"Fetching ratios for {symbol}...")
    try:
        df = fetch_financial_ratio_frame(symbol, period="Q")
        if df is None or df.empty:
            return pd.DataFrame()

        col_list = df.columns.tolist()

        def get_col(keyword):
            for c in col_list:
                if keyword in c:
                    return c
            return None

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
            "current_ratio": "Current Ratio",
            "quick_ratio": "Quick Ratio",
            "interest_coverage": "Interest Coverage",
            "asset_turnover": "Asset Turnover",
            "inventory_turnover": "Inventory Turnover",
            "receivable_turnover": "Receivable Turnover",
            "revenue_growth": "Revenue Growth",
            "profit_growth": "Profit Growth",
            "operating_margin": "Operating Margin",
            "gross_margin": "Gross Margin",
            "free_cash_flow": "Free Cash Flow",
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

        out_df["ticker"] = symbol
        out_df["asset_id"] = asset_id

        return out_df

    except Exception as e:
        logging.error(f"Error ratios {symbol}: {e}")
        return pd.DataFrame()


@cached_data(ttl_seconds=86400)  # 24 hours
def fetch_income_stmt(symbol, asset_id):
    """
    Fetches income statement.
    """
    try:
        df = fetch_income_statement_frame(symbol, period="Q")

        if df is None or df.empty:
            return pd.DataFrame()

        # Mapping
        mapping = {
            "Net Sales": "revenue",
            "Cost of Sales": "cost_of_goods_sold",
            "Gross Profit": "gross_profit",
            "Operating Profit/Loss": "operating_profit",
            "Net Profit For the Year": "net_profit_post_tax",
            "Selling Expenses": "selling_expenses",
            "Selling Expense": "selling_expenses",
            "General & Admin": "admin_expenses",
            "Admin Expense": "admin_expenses",
            "General & Admin Expense": "admin_expenses",
            "Financial Income": "financial_income",
            "Financial Expense": "financial_expenses",
            "Other Income": "other_income",
            "Other Expense": "other_expenses",
            "EBITDA": "ebitda",
        }

        # Safe rename: only rename columns that exist
        rename_dict = {col: mapping[col] for col in mapping if col in df.columns}
        df.rename(columns=rename_dict, inplace=True)

        # The required metrics are the unique values in the mapping dict
        required_metrics = list(set(mapping.values()))
        df_final = df.copy()

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

        df_final["ticker"] = symbol
        df_final["asset_id"] = asset_id

        # Select Final Columns
        final_cols = [
            "ticker",
            "asset_id",
            "fiscal_date",
            "year",
            "quarter",
        ] + required_metrics
        return df_final[final_cols]

    except Exception as e:
        logging.error(f"Error fetching income stmt for {symbol}: {e}", exc_info=True)
        return pd.DataFrame()


@cached_data(ttl_seconds=86400)  # 24 hours
def fetch_balance_sheet(symbol, asset_id):
    try:
        df = fetch_balance_sheet_frame(symbol, period="Q")

        if df is None or df.empty:
            return pd.DataFrame()

        mapping = {
            "Total Asset": "total_assets",
            "Total Assets": "total_assets",
            "Total Liabilities": "total_liabilities",
            "Owner's Equity": "total_equity",
            "Total Equity": "total_equity",
            "Cash & Equivalents": "cash_and_equivalents",
            "Cash and Cash Equivalents": "cash_and_equivalents",
            "Short-term Asset": "short_term_assets",
            "Short Term Assets": "short_term_assets",
            "Long-term Asset": "long_term_assets",
            "Long Term Assets": "long_term_assets",
            "Short-term Liability": "short_term_liabilities",
            "Short Term Liabilities": "short_term_liabilities",
            "Long-term Liability": "long_term_liabilities",
            "Long Term Liabilities": "long_term_liabilities",
        }

        rename_dict = {col: mapping[col] for col in mapping if col in df.columns}
        df.rename(columns=rename_dict, inplace=True)

        required_metrics = list(set(mapping.values()))
        df_final = df.copy()

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
                except Exception:
                    pass
                return None

            df_final["fiscal_date"] = df_final.apply(make_date, axis=1)

        df_final.dropna(subset=["year", "quarter", "fiscal_date"], inplace=True)

        for col in required_metrics:
            if col not in df_final.columns:
                df_final[col] = 0.0

        df_final = clean_decimal_cols(df_final, required_metrics)
        df_final["asset_id"] = asset_id

        final_cols = ["asset_id", "fiscal_date", "year", "quarter"] + required_metrics
        return df_final[final_cols]
    except Exception as e:
        logging.error(f"Error fetching balance sheet for {symbol}: {e}", exc_info=True)
        return pd.DataFrame()


@cached_data(ttl_seconds=86400)  # 24 hours
def fetch_corporate_events(symbol, asset_id):
    try:
        df = fetch_company_events(symbol)
        if df is None or df.empty:
            return pd.DataFrame()

        df_final = df.copy()
        df_final["asset_id"] = asset_id

        required_cols = [
            "asset_id",
            "event_id",
            "event_date",
            "public_date",
            "exright_date",
            "event_title",
            "event_type",
            "event_description",
        ]

        for col in required_cols:
            if col not in df_final.columns:
                df_final[col] = None

        if "event_id" in df_final.columns:
            df_final["event_id"] = df_final["event_id"].astype(str)

        for dcol in ["event_date", "public_date", "exright_date"]:
            if dcol in df_final.columns:
                df_final[dcol] = pd.to_datetime(df_final[dcol], errors="coerce").dt.date

        return df_final[required_cols]
    except Exception as e:
        if "404" in str(e) or "Not Found" in str(e):
            logging.warning(f"Events data not available for {symbol}")
        else:
            logging.error(f"Error fetching events for {symbol}: {e}", exc_info=True)
        return pd.DataFrame()


@cached_data(ttl_seconds=43200)  # 12 hours
def fetch_index_history(
    symbol: str, asset_id: str, start_date: str, end_date: str
) -> pd.DataFrame:
    """
    Fetch historical prices for a VN index (e.g. VNINDEX, VN30, HNXINDEX)
    using vnstock's stock quote API with the VCI source.

    The index is treated as a zero-dividend synthetic asset:
    adjusted_close == raw_close (no corporate action adjustment needed).
    Rows are stored in market_data.prices with source='vnstock_index'.

    Parameters
    ----------
    symbol : str
        Index ticker, e.g. 'VNINDEX', 'VN30', 'HNXINDEX'.
    start_date : str
        ISO date string, e.g. '2020-01-01'.
    end_date : str
        ISO date string, e.g. '2024-12-31'.

    Returns
    -------
    pd.DataFrame
        Columns matching prices schema with close/volume plus
        indicator columns set to 0.
    """
    logging.info(
        "Fetching index history for %s (%s → %s)", symbol, start_date, end_date
    )
    try:
        index_symbol_map = {
            "VNINDEX": "VNINDEX",
            "VN30": "VN30",
            "HNXINDEX": "HNXIndex",
            "UPCOMINDEX": "HNXUpcomIndex",
        }
        api_symbol = index_symbol_map.get(symbol.upper(), symbol)
        df = fetch_stock_price_frame(api_symbol, start_date, end_date)
        if df is None or df.empty:
            raise ValueError(f"Empty data returned for index {symbol}")

        df["ticker"] = symbol
        df["source"] = "vnstock_index"

        if "trading_date" in df.columns and not pd.api.types.is_datetime64_any_dtype(
            df["trading_date"]
        ):
            df["trading_date"] = pd.to_datetime(df["trading_date"])
        if "trading_date" in df.columns:
            df["trading_date"] = df["trading_date"].dt.date

        df = clean_decimal_cols(df, ["close"])
        df["volume"] = (
            df.get("volume", pd.Series(0, index=df.index)).fillna(0).astype(int)
        )

        required_cols = [
            "ticker",
            "trading_date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "source",
        ]
        for col in required_cols:
            if col not in df.columns:
                df[col] = 0
        return df[required_cols]

    except Exception as e:
        logging.error(
            "Error fetching index history for %s: %s", symbol, e, exc_info=True
        )
        return pd.DataFrame()


@cached_data(ttl_seconds=3600)  # 1 hour
def fetch_dividends(symbol, asset_id):
    """Fetch dividend history and normalize to a stable schema."""
    try:
        global Company
        if Company is None:
            from vnstock import Company as VnstockCompany

            Company = VnstockCompany

        company = Company(symbol=symbol, source="VCI")
        df = company.dividends()
        if df is None or df.empty:
            return pd.DataFrame()

        df = df.copy()
        df["ticker"] = symbol
        df["asset_id"] = asset_id

        # Normalize commonly seen provider column variants.
        rename_map = {
            "exerciseDate": "exercise_date",
            "exerDate": "exercise_date",
            "cashYear": "cash_year",
            "cashRate": "cash_dividend_percentage",
            "stockRate": "stock_dividend_percentage",
            "issueMethod": "issue_method",
        }
        for old_col, new_col in rename_map.items():
            if old_col in df.columns and new_col not in df.columns:
                df.rename(columns={old_col: new_col}, inplace=True)

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
                df[col] = None

        df["exercise_date"] = pd.to_datetime(
            df["exercise_date"], errors="coerce"
        ).dt.date
        df["cash_year"] = pd.to_numeric(df["cash_year"], errors="coerce").fillna(0)
        df["cash_year"] = df["cash_year"].astype(int)
        df = clean_decimal_cols(
            df, ["cash_dividend_percentage", "stock_dividend_percentage"]
        )

        return df[required_cols]
    except Exception as e:
        logging.error(f"Error fetching dividends for {symbol}: {e}", exc_info=True)
        return pd.DataFrame()


@cached_data(ttl_seconds=3600)  # 1 hour
def fetch_news(symbol, asset_id):
    try:
        df = fetch_company_news(symbol)
        if df is None or df.empty:
            return pd.DataFrame()

        df["ticker"] = symbol
        df["asset_id"] = asset_id
        if "news_title" in df.columns and "title" not in df.columns:
            df.rename(columns={"news_title": "title"}, inplace=True)
        if "price" in df.columns and "price_at_publish" not in df.columns:
            df.rename(columns={"price": "price_at_publish"}, inplace=True)
        if "close_price" in df.columns and "price_at_publish" not in df.columns:
            df["price_at_publish"] = pd.to_numeric(df["close_price"], errors="coerce")
        if "reference_price" in df.columns and "price_change" not in df.columns:
            df["price_change"] = pd.to_numeric(
                df.get("price_at_publish"), errors="coerce"
            ) - pd.to_numeric(df["reference_price"], errors="coerce")
        if (
            "percent_price_change" in df.columns
            and "price_change_ratio" not in df.columns
        ):
            df["price_change_ratio"] = pd.to_numeric(
                df["percent_price_change"], errors="coerce"
            )
        if "id" in df.columns and "news_id" not in df.columns:
            df.rename(columns={"id": "news_id"}, inplace=True)
        if "source" not in df.columns and "news_source_link" in df.columns:
            df["source"] = df["news_source_link"].apply(_extract_source_host)
        df["publish_date"] = pd.to_datetime(df["publish_date"], errors="coerce")
        df = clean_decimal_cols(
            df, ["price_at_publish", "price_change", "price_change_ratio", "rsi", "rs"]
        )
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
                df[col] = (
                    0.0
                    if col
                    in {
                        "price_at_publish",
                        "price_change",
                        "price_change_ratio",
                        "rsi",
                        "rs",
                    }
                    else None
                )
        return df[required_cols]
    except SystemExit as e:
        logging.warning(f"Rate-limited while fetching news for {symbol}: {e}")
        return pd.DataFrame()
    except Exception as e:
        logging.error(f"Error fetching news for {symbol}: {e}", exc_info=True)
        return pd.DataFrame()
