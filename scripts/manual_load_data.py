import argparse
import pandas as pd
import clickhouse_connect
import os
import sys
import logging

# Add project root to path to import dags.etl_modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dags.etl_modules.fetcher import (
    fetch_stock_price,
    fetch_financial_ratios,
    fetch_dividends,
    fetch_income_stmt,
    fetch_news,
)

try:
    from vnstock import Listing
except ImportError:
    logging.warning("vnstock not installed, company dimension update will fail.")
    Listing = None

# CONFIG
STOCKS = ["HPG", "VCB", "VNM", "FPT", "MWG", "VIC"]
CLICKHOUSE_HOST = os.getenv(
    "CLICKHOUSE_HOST", "localhost"
)  # Default to localhost for manual script
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", 8123))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def manual_load(start_date, end_date, price_only=False, ratios_only=False):
    logging.info(f"Starting manual load from {start_date} to {end_date}")

    # Determine what to fetch
    fetch_price = not ratios_only
    fetch_ratios = not price_only
    fetch_divs = not (price_only or ratios_only)
    fetch_income = not (price_only or ratios_only)
    fetch_news_flag = not (price_only or ratios_only)

    price_data, ratio_data, div_data, income_data, news_data = [], [], [], [], []

    for ticker in STOCKS:
        if fetch_price:
            df = fetch_stock_price(ticker, start_date, end_date)
            if not df.empty:
                price_data.append(df)
        if fetch_ratios:
            df = fetch_financial_ratios(ticker)
            if not df.empty:
                ratio_data.append(df)
        if fetch_divs:
            df = fetch_dividends(ticker)
            if not df.empty:
                div_data.append(df)
        if fetch_income:
            df = fetch_income_stmt(ticker)
            if not df.empty:
                income_data.append(df)
        if fetch_news_flag:
            df = fetch_news(ticker)
            if not df.empty:
                news_data.append(df)

    try:
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
        )
    except Exception as e:
        logging.error(f"Connection failed: {e}")
        return

    # 1. LOAD PRICES (Added MACD)
    if price_data:
        df = pd.concat(price_data)
        price_cols = [
            "ticker",
            "trading_date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "ma_50",
            "ma_200",
            "rsi_14",
            "daily_return",
            "macd",
            "macd_signal",
            "macd_hist",
            "source",
        ]
        # Ensure new cols exist
        for c in ["macd", "macd_signal", "macd_hist", "source"]:
            if c not in df.columns:
                if c == "source":
                    df[c] = "vnstock"
                else:
                    df[c] = 0.0

        # Ensure ticker and source are strings
        df["ticker"] = df["ticker"].astype(str)
        df["source"] = df["source"].astype(str)

        # Ensure trading_date is a date object
        df["trading_date"] = pd.to_datetime(df["trading_date"]).dt.date

        # Ensure volume is integer
        df["volume"] = df["volume"].fillna(0).astype(int)

        # Replace NaN/None with 0 for numeric columns
        numeric_cols = [
            "open",
            "high",
            "low",
            "close",
            "ma_50",
            "ma_200",
            "rsi_14",
            "daily_return",
            "macd",
            "macd_signal",
            "macd_hist",
        ]
        df[numeric_cols] = df[numeric_cols].fillna(0.0).astype(float)

        # Select only the columns we need in the correct order
        df = df[price_cols]

        # Convert to list of lists (not dict) for better type control
        data = df.values.tolist()

        logging.info(f"Inserting {len(data)} price rows...")
        client.insert("market_dwh.fact_stock_daily", data, column_names=price_cols)
        client.command("OPTIMIZE TABLE market_dwh.fact_stock_daily FINAL")

    # 2. LOAD RATIOS
    if ratio_data:
        df = pd.concat(ratio_data, ignore_index=True)

        # Debug: Check ticker values before processing
        logging.info(
            f"Ticker values before processing: {df['ticker'].unique()[:5] if 'ticker' in df.columns else 'No ticker column'}"
        )

        ratio_cols = [
            "ticker",
            "fiscal_date",
            "year",
            "quarter",
            "pe_ratio",
            "pb_ratio",
            "ps_ratio",
            "p_cashflow_ratio",
            "eps",
            "bvps",
            "market_cap",
            "roe",
            "roa",
            "roic",
            "net_profit_margin",
            "debt_to_equity",
            "financial_leverage",
            "dividend_yield",
        ]
        # Fill missing new cols with 0
        for c in ratio_cols:
            if c not in df.columns:
                if c == "ticker":
                    logging.error("ticker column is missing!")
                df[c] = 0.0 if c != "ticker" else ""

        # Ensure ticker is string type (handle NaN properly)
        if "ticker" in df.columns:
            # Replace NaN in ticker column with empty string before converting to str
            df["ticker"] = df["ticker"].fillna("UNKNOWN").astype(str)

        # Ensure fiscal_date is a date object
        df["fiscal_date"] = pd.to_datetime(df["fiscal_date"]).dt.date

        # Ensure year and quarter are integers
        df["year"] = df["year"].fillna(0).astype(int)
        df["quarter"] = df["quarter"].fillna(0).astype(int)

        # Replace NaN/None with 0 for numeric columns
        numeric_cols = [
            c
            for c in ratio_cols
            if c not in ["ticker", "fiscal_date", "year", "quarter"]
        ]
        df[numeric_cols] = df[numeric_cols].fillna(0.0).astype(float)

        # Select only the columns we need in the correct order
        df = df[ratio_cols]

        # Convert to list of lists (not dict) for better type control
        data = df.values.tolist()

        logging.info(f"Inserting {len(data)} ratio rows...")
        client.insert("market_dwh.fact_financial_ratios", data, column_names=ratio_cols)
        client.command("OPTIMIZE TABLE market_dwh.fact_financial_ratios FINAL")

    # Load Dividends
    if fetch_divs:
        if div_data:
            final_div_df = pd.concat(div_data)
            # Ensure date format is correct
            final_div_df["exercise_date"] = pd.to_datetime(
                final_div_df["exercise_date"]
            ).dt.date

            div_records = final_div_df.to_dict("records")
            logging.info(f"Inserting {len(div_records)} dividend rows...")

            div_cols = [
                "ticker",
                "exercise_date",
                "cash_year",
                "cash_dividend_percentage",
                "stock_dividend_percentage",
                "issue_method",
            ]
            div_tuples = []
            for row in div_records:
                div_tuples.append([row.get(col) for col in div_cols])

            client.insert(
                "market_dwh.fact_dividends", div_tuples, column_names=div_cols
            )
            logging.info("Dividend insertion complete.")
            client.command("OPTIMIZE TABLE market_dwh.fact_dividends FINAL")
        else:
            logging.warning("No dividend data fetched.")

    # Load Income Statement
    if fetch_income:
        if income_data:
            final_inc_df = pd.concat(income_data)
            final_inc_df["fiscal_date"] = pd.to_datetime(
                final_inc_df["fiscal_date"]
            ).dt.date

            inc_records = final_inc_df.to_dict("records")
            logging.info(f"Inserting {len(inc_records)} income statement rows...")

            inc_cols = [
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
            inc_tuples = []
            for row in inc_records:
                # Handle None values for numeric columns (ClickHouse Decimal doesn't like None)
                vals = []
                for col in inc_cols:
                    val = row.get(col)
                    # If value is None and it's a metric column (not metadata), default to 0
                    if val is None and col not in [
                        "ticker",
                        "fiscal_date",
                        "year",
                        "quarter",
                    ]:
                        val = 0
                    vals.append(val)
                inc_tuples.append(vals)

            client.insert(
                "market_dwh.fact_income_statement", inc_tuples, column_names=inc_cols
            )
            logging.info("Income statement insertion complete.")
            client.command("OPTIMIZE TABLE market_dwh.fact_income_statement FINAL")
        else:
            logging.warning("No income statement data fetched.")

    # Load News
    if fetch_news_flag:
        if news_data:
            final_news_df = pd.concat(news_data)
            # Ensure publish_date is datetime
            final_news_df["publish_date"] = pd.to_datetime(
                final_news_df["publish_date"]
            )

            news_records = final_news_df.to_dict("records")
            logging.info(f"Inserting {len(news_records)} news rows...")

            news_cols = [
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
            news_tuples = []
            for row in news_records:
                news_tuples.append([row.get(col) for col in news_cols])

            client.insert("market_dwh.fact_news", news_tuples, column_names=news_cols)
            logging.info("News insertion complete.")
            client.command("OPTIMIZE TABLE market_dwh.fact_news FINAL")
        else:
            logging.warning("No news data fetched.")


def update_company_dimension(client):
    if not Listing:
        logging.error("vnstock not installed.")
        return

    logging.info("Fetching company list & industry mapping...")
    try:
        listing = Listing(source="VCI")

        # 1. Get List
        df_list = listing.symbols_by_exchange()
        if df_list.empty:
            logging.warning("No symbols found.")
            return

        # Filter out DELISTED companies (case-insensitive check)
        if "exchange" in df_list.columns:
            df_list = df_list[df_list["exchange"].str.upper() != "DELISTED"]
            logging.info(
                f"Filtered to {len(df_list)} active companies (excluding delisted)"
            )

        # 2. Get Industries
        try:
            df_ind = listing.symbols_by_industries()
        except Exception:
            logging.warning("Failed to fetch industries, defaulting to Unknown.")
            df_ind = pd.DataFrame()

        # 3. Merge Logic
        if not df_ind.empty:
            # Join Industry data
            df_merged = pd.merge(
                df_list[["symbol", "organ_name", "exchange"]],
                df_ind[["symbol", "icb_name2", "icb_name3", "icb_name4", "icb_code3"]],
                on="symbol",
                how="left",
            )
            # Rename to match ClickHouse Schema
            df_merged.rename(
                columns={
                    "icb_name2": "sector",
                    "icb_name3": "industry",
                    "icb_name4": "sub_industry",
                    "icb_code3": "icb_code",
                },
                inplace=True,
            )
            # Fill missing INDUSTRY data
            df_merged.fillna(
                {
                    "sector": "Unknown",
                    "industry": "Unknown",
                    "sub_industry": "",
                    "icb_code": "",
                },
                inplace=True,
            )
        else:
            # Fallback if industry API fails
            df_merged = df_list[["symbol", "organ_name", "exchange"]].copy()
            df_merged["sector"] = "Unknown"
            df_merged["industry"] = "Unknown"
            df_merged["sub_industry"] = ""
            df_merged["icb_code"] = ""

        # ClickHouse String cannot accept NaN/None.
        df_merged["organ_name"] = df_merged["organ_name"].fillna("").astype(str)

        # Ensure 'icb_code' is string (sometimes it comes as int/float)
        df_merged["icb_code"] = df_merged["icb_code"].astype(str).replace("nan", "")

        # 4. Insert
        # insert_df automatically maps DataFrame columns to Table columns by name.
        # It handles the missing 'ingested_at' by letting ClickHouse use the Default value.
        logging.info(f"Inserting {len(df_merged)} rows into dim_stock_companies...")

        client.insert_df("market_dwh.dim_stock_companies", df_merged)

        # Deduplicate immediately
        client.command("OPTIMIZE TABLE market_dwh.dim_stock_companies FINAL")
        logging.info("Company dimension updated successfully.")

    except Exception as e:
        logging.error(f"Dimension update failed: {e}", exc_info=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Manual Stock Data Loader")

    # Mutually exclusive group for mode
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--price-only", action="store_true", help="Fetch only stock prices"
    )
    group.add_argument(
        "--ratios-only", action="store_true", help="Fetch only financial ratios"
    )

    parser.add_argument(
        "--update-companies",
        action="store_true",
        help="Update company dimension table from vnstock",
    )
    parser.add_argument("--start", required=False, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", required=False, help="End date (YYYY-MM-DD)")

    args = parser.parse_args()

    if args.update_companies:
        # Create client just for this operation if running from CLI
        try:
            client = clickhouse_connect.get_client(
                host=CLICKHOUSE_HOST,
                port=CLICKHOUSE_PORT,
                username=CLICKHOUSE_USER,
                password=CLICKHOUSE_PASSWORD,
            )
            update_company_dimension(client)
        except Exception as e:
            logging.error(f"Failed to connect to ClickHouse: {e}")

    if args.start and args.end:
        manual_load(args.start, args.end, args.price_only, args.ratios_only)
