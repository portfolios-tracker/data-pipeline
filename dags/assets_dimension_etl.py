"""
Asset Dimension ETL DAG
Story: prep-3-seed-asset-data
Architecture: Single Table Inheritance (STI)

Fetches:
- VN stocks (vnstock) → asset_class=STOCK, market=VN
- US stocks (Wikipedia S&P 500 + yfinance) → asset_class=STOCK, market=US
- Crypto (CoinGecko) → asset_class=CRYPTO, market=NULL

Schedule: Weekly (Sunday 2 AM)
"""

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import clickhouse_connect
import pandas as pd
import os
import logging
from io import StringIO
# ... (existing imports preserved implicitly via context, but I will just update the specific lines)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    "owner": "data-pipeline",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "assets_dimension_etl",
    default_args=default_args,
    description="Refresh asset master data (STI pattern: asset_class + market)",
    schedule="0 2 * * 0",  # Weekly Sunday 2 AM
    catchup=False,
    tags=["assets", "dimension", "etl", "sti"],
)


def get_clickhouse_client():
    """Create ClickHouse client with environment configuration"""
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "clickhouse-server"),
        port=int(os.getenv("CLICKHOUSE_PORT", 8123)),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
    )


def fetch_vn_stocks(**context):
    """
    Fetch VN stocks from vnstock
    → asset_class = 'STOCK', market = 'VN'
    """
    from vnstock import Listing

    client = get_clickhouse_client()
    listing = Listing(source="VCI")

    logger.info("Fetching VN stock list from vnstock...")

    # Get all stocks
    df_list = listing.symbols_by_exchange()
    df_list = df_list[df_list["exchange"].str.upper() != "DELISTED"]

    logger.info(f"Found {len(df_list)} active VN stocks")

    # Get industry data
    try:
        df_ind = listing.symbols_by_industries()
        df = pd.merge(
            df_list[["symbol", "organ_name", "exchange"]],
            df_ind[["symbol", "icb_name2", "icb_name3"]],
            on="symbol",
            how="left",
        )
        logger.info("Successfully merged industry data")
    except Exception as e:
        logger.warning(f"Failed to fetch industries: {e}, using fallback")
        df = df_list[["symbol", "organ_name", "exchange"]].copy()
        df["icb_name2"] = "Unknown"
        df["icb_name3"] = "Unknown"

    # Transform to STI schema
    records = []
    for _, row in df.iterrows():
        records.append(
            {
                "symbol": str(row["symbol"]),
                # vnstock only provides 'organ_name' (Vietnamese).
                # We use it for both name_en and name_local to ensure UI has a display name.
                # TODO: (Improvement) find api to get correct name_en of vn stocks
                "name_en": str(row["organ_name"])
                if pd.notna(row["organ_name"])
                else "",
                "name_local": str(row["organ_name"])
                if pd.notna(row["organ_name"])
                else "",
                "asset_class": "STOCK",  # STI discriminator
                "market": "VN",  # Market code
                "currency": "VND",
                "exchange": str(row["exchange"])
                if pd.notna(row["exchange"])
                else "HOSE",
                "sector": str(row["icb_name2"])
                if pd.notna(row["icb_name2"])
                else "Unknown",
                "industry": str(row["icb_name3"])
                if pd.notna(row["icb_name3"])
                else "Unknown",
                "logo_url": "",
                "description": "",
                "external_api_metadata": {"source_api": "vnstock"},
                "source": "vnstock",
                "is_active": 1,
            }
        )

    df_ch = pd.DataFrame(records)

    client.insert_df("market_dwh.dim_assets", df_ch)
    client.command("OPTIMIZE TABLE market_dwh.dim_assets FINAL")

    logger.info(f"✅ Inserted {len(df_ch)} VN stocks (asset_class=STOCK, market=VN)")
    return len(df_ch)


def fetch_us_stocks(**context):
    """
    Fetch US stocks using HYBRID APPROACH (Wikipedia S&P 500 + yfinance)
    → asset_class = 'STOCK', market = 'US'
    """
    import yfinance as yf

    client = get_clickhouse_client()

    logger.info("Fetching S&P 500 constituents from Wikipedia...")

    # Step 1: Get official S&P 500 constituents
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    import requests

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    sp500_df = pd.read_html(StringIO(response.text))[0]

    logger.info(f"Found {len(sp500_df)} S&P 500 constituents")

    # Step 2: Fetch metadata and market caps
    enriched_data = []
    for idx, row in sp500_df.iterrows():
        symbol = row["Symbol"]
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info

            enriched_data.append(
                {
                    "symbol": symbol,
                    "name_en": info.get("longName", row["Security"]),
                    "market_cap": info.get("marketCap", 0) or 0,
                    "sector": info.get("sector", row.get("GICS Sector", "Unknown")),
                    "industry": info.get(
                        "industry", row.get("GICS Sub-Industry", "Unknown")
                    ),
                    "exchange": info.get("exchange", "NASDAQ"),
                    "external_api_metadata": {
                        "yfinance_ticker": symbol,
                        "isin": info.get("isin", ""),
                        "cik": str(row.get("CIK", "")),
                    },
                }
            )

            if idx % 50 == 0:
                logger.info(f"Processed {idx}/{len(sp500_df)} stocks...")

        except Exception as e:
            logger.warning(f"⚠️ Failed to fetch {symbol}: {e}")

    logger.info(f"Successfully enriched {len(enriched_data)} stocks")

    # Step 3: Use all S&P 500 stocks
    df = pd.DataFrame(enriched_data)

    # Step 4: Format for STI schema
    records = []
    for _, row in df.iterrows():
        records.append(
            {
                "symbol": str(row["symbol"]),
                "name_en": str(row["name_en"]) if pd.notna(row["name_en"]) else "",
                "name_local": "",  # US stocks don't have local names
                "asset_class": "STOCK",  # STI discriminator
                "market": "US",  # Market code
                "currency": "USD",
                "exchange": str(row["exchange"])
                if pd.notna(row["exchange"])
                else "NASDAQ",
                "sector": str(row["sector"]) if pd.notna(row["sector"]) else "Unknown",
                "industry": str(row["industry"])
                if pd.notna(row["industry"])
                else "Unknown",
                "logo_url": "",
                "description": "",
                "external_api_metadata": row["external_api_metadata"],
                "source": "yfinance",
                "is_active": 1,
            }
        )

    df_ch = pd.DataFrame(records)

    client.insert_df("market_dwh.dim_assets", df_ch)
    client.command("OPTIMIZE TABLE market_dwh.dim_assets FINAL")

    logger.info(f"✅ Inserted {len(df_ch)} US stocks (asset_class=STOCK, market=US)")
    return len(df_ch)


def fetch_crypto(**context):
    """
    Fetch top 500 crypto from CoinGecko
    → asset_class = 'CRYPTO', market = '' (empty/NULL)
    NOTE: 'coingecko_id' in metadata is CRITICAL for MarketDataService price fetching.
    """
    from pycoingecko import CoinGeckoAPI
    import time

    client = get_clickhouse_client()
    cg = CoinGeckoAPI()

    logger.info("Fetching top 500 crypto from CoinGecko...")

    all_coins = []
    # Fetch top 500 (2 pages of 250)
    for page in range(1, 3):
        try:
            logger.info(f"Fetching page {page}...")
            coins = cg.get_coins_markets(
                vs_currency="usd",
                order="market_cap_desc",
                per_page=250,
                page=page,
                sparkline=False,
            )
            all_coins.extend(coins)
            time.sleep(1) # Rate limit protection
        except Exception as e:
            logger.error(f"Failed to fetch page {page}: {e}")

    logger.info(f"Fetched {len(all_coins)} crypto assets")

    # Known stablecoins
    stablecoins = {"USDT", "USDC", "DAI", "BUSD", "TUSD", "USDD", "FRAX", "USDP", "PYUSD"}

    records = []
    # Track symbols to prevent duplicates if any
    seen_symbols = set()

    for coin in all_coins:
        symbol_upper = coin["symbol"].upper()
        
        if symbol_upper in seen_symbols:
            continue
        seen_symbols.add(symbol_upper)

        is_stable = "1" if symbol_upper in stablecoins else "0"

        records.append(
            {
                "symbol": symbol_upper,
                "name_en": coin["name"],
                "name_local": "",
                "asset_class": "CRYPTO",  # STI discriminator
                "market": "",  # No market for crypto
                "currency": "USD",
                "exchange": "",  # Crypto trades on multiple exchanges
                "sector": "Cryptocurrency",
                "industry": "",
                "logo_url": coin.get("image", ""),
                "description": "",
                "external_api_metadata": {
                    "coingecko_id": coin["id"],
                    "chain": "",  # Would require additional API call
                    "is_stablecoin": is_stable,
                    "market_cap_rank": str(coin.get("market_cap_rank", 0)),
                },
                "source": "coingecko",
                "is_active": 1,
            }
        )

    df = pd.DataFrame(records)

    client.insert_df("market_dwh.dim_assets", df)
    client.command("OPTIMIZE TABLE market_dwh.dim_assets FINAL")

    logger.info(f"✅ Inserted {len(df)} crypto (asset_class=CRYPTO)")
    return len(df)


# Define tasks
task_vn = PythonOperator(
    task_id="fetch_vn_stocks",
    python_callable=fetch_vn_stocks,
    dag=dag,
)

task_us = PythonOperator(
    task_id="fetch_us_stocks",
    python_callable=fetch_us_stocks,
    dag=dag,
)

task_crypto = PythonOperator(
    task_id="fetch_crypto",
    python_callable=fetch_crypto,
    dag=dag,
)

# All tasks run in parallel
[task_vn, task_us, task_crypto]
