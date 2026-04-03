import pandas as pd
import logging
import os
import numpy as np
from supabase import create_client
from vnstock import Quote, Finance, Company
from urllib.parse import urlparse
from .cache import cached_data


# ---------------------------------------------------------------------------
# Fallback ticker list used when Supabase is unreachable during DAG parsing
# ---------------------------------------------------------------------------
_FALLBACK_VN_TICKERS = ["HPG", "VCB", "VNM", "FPT", "MWG", "VIC"]


def get_active_vn_stock_tickers(raise_on_fallback: bool = False) -> list[dict[str, str]]:
    """
    Return active VN STOCK tickers from the Supabase ``market_data.assets`` table.

    Falls back to a hardcoded seed list when Supabase is not reachable
    (e.g. during DAG file parsing on Airflow startup).

    Parameters
    ----------
    raise_on_fallback : bool
        When True, raise a RuntimeError instead of returning the fallback list.
        Set this to True in task execution contexts to prevent silent partial
        ingestion when Supabase is unexpectedly unreachable at runtime.
    """
    try:
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_SECRET_OR_SERVICE_ROLE_KEY")
        if not url or not key:
            raise ValueError(
                "SUPABASE_URL or SUPABASE_SECRET_OR_SERVICE_ROLE_KEY not set"
            )
        client = create_client(url, key)
        response = (
            client.table("market_data.assets")
            .select("id, symbol")
            .eq("asset_class", "STOCK")
            .eq("market", "VN")
            .order("symbol")
            .execute()
        )
        
        tickers = []
        seen_symbols = set()
        for row in response.data:
            symbol = row.get("symbol")
            if symbol:
                cleaned = str(symbol).strip().upper()
                if cleaned and cleaned not in seen_symbols:
                    tickers.append({"symbol": cleaned, "asset_id": row["id"]})
                    seen_symbols.add(cleaned)

        if tickers:
            logging.info(f"Loaded {len(tickers)} active VN tickers from assets table")
            return tickers
        # Treat an empty result set as a fallback condition as well
        if raise_on_fallback:
            raise RuntimeError(
                "Supabase market_data.assets query returned zero active VN stock tickers "
                "for market=VN and asset_class=STOCK. "
                "Task will fail so Airflow can retry. "
                "If this is persistent, check assets table contents and filters."
            )
        logging.warning(
            "Supabase market_data.assets query returned zero active VN stock tickers, "
            f"falling back to seed list of {len(_FALLBACK_VN_TICKERS)} tickers "
            "(partial ingestion risk)."
        )
    except Exception as e:
        if raise_on_fallback:
            raise RuntimeError(
                f"Could not load active VN stock tickers from Supabase market_data.assets table: {e}. "
                "Task will fail so Airflow can retry. "
                "If this is persistent, check Supabase connectivity and the assets table."
            ) from e
        logging.error(
            f"Could not query market_data.assets table, falling back to seed list of "
            f"{len(_FALLBACK_VN_TICKERS)} tickers (partial ingestion risk): {e}"
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

        # Now fetching full OHLCV
        required_cols = ["trading_date", "open", "high", "low", "close", "volume"]
        df = df[[c for c in required_cols if c in df.columns]]
        df["asset_id"] = asset_id
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
    df = clean_decimal_cols(df, ["close"])
    df["volume"] = df["volume"].fillna(0).astype(int)

    return df


@cached_data(ttl_seconds=86400)  # 24 hours
def fetch_financial_ratios(symbol, asset_id):
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
            "Selling Expenses": "selling_expenses",
            "Selling Expense": "selling_expenses",
            "General & Admin": "admin_expenses",
            "Admin Expense": "admin_expenses",
            "General & Admin Expense": "admin_expenses",
            "Financial Income": "financial_income",
            "Financial Expense": "financial_expenses",
            "Other Income": "other_income",
            "Other Expense": "other_expenses",
            "EBITDA": "ebitda"
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
        
        df_final["asset_id"] = asset_id

        # Select Final Columns
        final_cols = ["asset_id", "fiscal_date", "year", "quarter"] + required_metrics
        return df_final[final_cols]

    except Exception as e:
        logging.error(f"Error fetching income stmt for {symbol}: {e}", exc_info=True)
        return pd.DataFrame()


@cached_data(ttl_seconds=86400)  # 24 hours
def fetch_balance_sheet(symbol, asset_id):
    try:
        finance = Finance(symbol=symbol, source="VCI")
        try:
            df = finance.balance_sheet(period="quarter", lang="en", dropna=True)
        except AttributeError:
            return pd.DataFrame()

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
                    if q == 1: return f"{y}-03-31"
                    if q == 2: return f"{y}-06-30"
                    if q == 3: return f"{y}-09-30"
                    if q == 4: return f"{y}-12-31"
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
        company = Company(symbol=symbol, source="TCBS")
        df = company.events()
        if df is None or df.empty:
            return pd.DataFrame()

        df_final = df.copy()
        df_final["asset_id"] = asset_id
        
        mapping = {
            "id": "event_id",
            "en__event_title": "event_title",
            "event_title": "event_title"
        }
        
        # Only rename if column exists
        for old_col, new_col in mapping.items():
            if old_col in df_final.columns and new_col not in df_final.columns:
                df_final.rename(columns={old_col: new_col}, inplace=True)
        
        date_mapping = {
            "exerDate": "event_date",
            "exerciseDate": "event_date",
            "exrightDate": "exright_date",
            "publicDate": "public_date",
        }
        for old_col, new_col in date_mapping.items():
            if old_col in df_final.columns and new_col not in df_final.columns:
                df_final.rename(columns={old_col: new_col}, inplace=True)

        type_mapping = {
            "eventCode": "event_type",
            "eventType": "event_type",
            "eventDesc": "event_description",
        }
        for old_col, new_col in type_mapping.items():
            if old_col in df_final.columns and new_col not in df_final.columns:
                df_final.rename(columns={old_col: new_col}, inplace=True)
            
        required_cols = ["asset_id", "event_id", "event_date", "public_date", "exright_date", "event_title", "event_type", "event_description"]
        
        for col in required_cols:
            if col not in df_final.columns:
                df_final[col] = None
                
        if "event_id" in df_final.columns:
            df_final["event_id"] = df_final["event_id"].astype(str)
            
        for dcol in ["event_date", "public_date", "exright_date"]:
            if dcol in df_final.columns:
                df_final[dcol] = pd.to_datetime(df_final[dcol], errors='coerce').dt.date

        return df_final[required_cols]
    except Exception as e:
        if "404" in str(e) or "Not Found" in str(e):
            logging.warning(f"Events data not available for {symbol}")
        else:
            logging.error(f"Error fetching events for {symbol}: {e}", exc_info=True)
        return pd.DataFrame()


@cached_data(ttl_seconds=43200)  # 12 hours
def fetch_index_history(symbol: str, asset_id: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Fetch historical prices for a VN index (e.g. VNINDEX, VN30, HNXINDEX)
    using vnstock's stock quote API with the VCI source.

    The index is treated as a zero-dividend synthetic asset:
    adjusted_close == raw_close (no corporate action adjustment needed).
    Rows are stored in market_data.market_data_prices with source='vnstock_index'.

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
        Columns matching market_data_prices schema with close/volume plus
        indicator columns set to 0.
    """
    logging.info("Fetching index history for %s (%s → %s)", symbol, start_date, end_date)
    try:
        from vnstock import Vnstock

        stock = Vnstock().stock(symbol=symbol, source="VCI")
        df = stock.quote.history(start=start_date, end=end_date, interval="1D")
        if df is None or df.empty:
            raise ValueError(f"Empty data returned for index {symbol}")

        df.columns = [c.lower() for c in df.columns]
        df.rename(columns={"time": "trading_date", "date": "trading_date"}, inplace=True)

        if not pd.api.types.is_datetime64_any_dtype(df["trading_date"]):
            df["trading_date"] = pd.to_datetime(df["trading_date"])
        df["trading_date"] = df["trading_date"].dt.date

        df["ticker"] = symbol
        df["source"] = "vnstock_index"

        df = clean_decimal_cols(df, ["close"])
        df["volume"] = df.get("volume", pd.Series(0, index=df.index)).fillna(0).astype(int)

        required_cols = [
            "ticker", "trading_date", "open", "high", "low", "close", "volume",
            "source",
        ]
        for col in required_cols:
            if col not in df.columns:
                df[col] = 0
        return df[required_cols]

    except Exception as e:
        logging.error("Error fetching index history for %s: %s", symbol, e, exc_info=True)
        return pd.DataFrame()


@cached_data(ttl_seconds=3600)  # 1 hour
def fetch_news(symbol, asset_id):
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
            df["price_change"] = pd.to_numeric(
                df["close_price"], errors="coerce"
            ) - pd.to_numeric(df["ref_price"], errors="coerce")
        if "price_change_pct" in df.columns:
            df["price_change_ratio"] = pd.to_numeric(
                df["price_change_pct"], errors="coerce"
            )

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
                    df["publish_date"] = pd.to_datetime(
                        df["publish_date"], unit="ms", errors="coerce"
                    )
                else:
                    df["publish_date"] = pd.to_datetime(
                        df["publish_date"], errors="coerce"
                    )
            except Exception:
                df["publish_date"] = pd.to_datetime(df["publish_date"], errors="coerce")

        required_cols = [
            "ticker",
            "asset_id",
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
    except SystemExit as e:
        # vnstock raises SystemExit when rate limit is exceeded.
        # Return empty data so the DAG can continue and persist partial results.
        logging.warning(f"Rate-limited while fetching news for {symbol}: {e}")
        return pd.DataFrame()
    except Exception as e:
        logging.error(f"Error fetching news for {symbol}: {e}", exc_info=True)
        return pd.DataFrame()

