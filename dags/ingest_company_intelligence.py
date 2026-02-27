"""
Company Intelligence Ingestion DAG
Story: 1-2-semantic-knowledge-ingestion-airflow-and-supabase

Fetches VN company profiles from vnstock (VCI source), generates 1536-dimensional
embeddings via OpenAI text-embedding-3-small, and upserts to Supabase pgvector.

Bonus: Backfills empty description field in ClickHouse dim_assets.

Schedule: Weekly Sunday 4 AM (after assets_dimension_etl at 2 AM)
"""

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import os
import logging
import time

import clickhouse_connect
from openai import OpenAI
from supabase import create_client, Client
from vnstock import Company

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
    "ingest_company_intelligence",
    default_args=default_args,
    description="Ingest VN company profiles → OpenAI embeddings → Supabase pgvector",
    schedule="0 4 * * 0",
    catchup=False,
    tags=["ai", "embeddings", "company", "supabase", "pgvector"],
)


def get_clickhouse_client():
    """Create ClickHouse client with environment configuration."""
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "clickhouse-server"),
        port=int(os.getenv("CLICKHOUSE_PORT", 8123)),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
    )


def get_supabase_client() -> Client:
    """Create Supabase client with environment configuration."""
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SECRET_OR_SERVICE_ROLE_KEY")
    if not url or not key:
        raise ValueError(
            "SUPABASE_URL or SUPABASE_SECRET_OR_SERVICE_ROLE_KEY environment variable not set"
        )
    return create_client(url, key)


def build_embedding_text(company_profile: str, icb_name2: str, icb_name3: str, icb_name4: str) -> str:
    """
    Concatenate company profile text with sector taxonomy for embedding.

    Why: Embedding the sector context alongside the profile text improves
    semantic search quality for thematic clustering.
    """
    sector_parts = [p for p in [icb_name2, icb_name3, icb_name4] if p and p.strip()]
    sector_str = " > ".join(sector_parts) if sector_parts else "Unknown"
    return f"{company_profile}\n\nSector: {sector_str}"


def fetch_company_profiles(**context):
    """
    Task 1: Query active VN tickers from ClickHouse and fetch company profiles via vnstock.

    - Rate limited: 0.5s sleep between API calls
    - Logs progress every 50 tickers
    - Skips tickers where vnstock returns no data
    """
    ch_client = get_clickhouse_client()

    query = """
    SELECT symbol, exchange
    FROM market_dwh.dim_assets
    WHERE asset_class = 'STOCK'
      AND market = 'VN'
      AND is_active = 1
    ORDER BY symbol
    """

    logger.info("Querying active VN tickers from ClickHouse...")
    result = ch_client.query(query)
    tickers = [(row[0], row[1]) for row in result.result_rows]
    logger.info(f"Found {len(tickers)} active VN tickers")

    profiles = []

    for idx, (symbol, exchange) in enumerate(tickers):
        try:
            company = Company(symbol=symbol, source="VCI")
            overview = company.overview()

            if overview is None or overview.empty:
                logger.warning(f"No overview data for {symbol}, skipping")
                time.sleep(0.5)
                continue

            row = overview.iloc[0]
            company_profile = str(row.get("company_profile", "") or "").strip()
            icb_name2 = str(row.get("icb_name2", "") or "").strip()
            icb_name3 = str(row.get("icb_name3", "") or "").strip()
            icb_name4 = str(row.get("icb_name4", "") or "").strip()

            if not company_profile:
                logger.warning(f"Empty company_profile for {symbol}, skipping")
                time.sleep(0.5)
                continue

            profiles.append({
                "ticker_symbol": symbol,
                "exchange": exchange,
                "content_type": "company_profile",
                "company_profile": company_profile,
                "icb_name2": icb_name2,
                "icb_name3": icb_name3,
                "icb_name4": icb_name4,
            })

            if (idx + 1) % 50 == 0:
                logger.info(f"Processed {idx + 1}/{len(tickers)} tickers...")

        except Exception as e:
            logger.warning(f"Failed to fetch profile for {symbol}: {e}")

        time.sleep(0.5)

    logger.info(f"Successfully fetched {len(profiles)} company profiles")

    context["ti"].xcom_push(key="profiles", value=profiles)
    return len(profiles)


def generate_and_upsert_embeddings(**context):
    """
    Task 2 + 3: Generate embeddings via OpenAI text-embedding-3-small and upsert to Supabase.

    - Batches embedding API calls in groups of 100
    - Upserts to Supabase company_embeddings in batches of 100
    - Includes ICB sector metadata in JSONB
    """
    profiles = context["ti"].xcom_pull(key="profiles", task_ids="fetch_company_profiles")

    if not profiles:
        logger.warning("No profiles received from XCom, skipping embeddings")
        return 0

    openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    supabase = get_supabase_client()

    texts = [
        build_embedding_text(
            p["company_profile"], p["icb_name2"], p["icb_name3"], p["icb_name4"]
        )
        for p in profiles
    ]

    logger.info(f"Generating embeddings for {len(texts)} profiles in batches of 100...")

    all_embeddings = []
    batch_size = 100

    for i in range(0, len(texts), batch_size):
        batch_texts = texts[i: i + batch_size]
        try:
            response = openai_client.embeddings.create(
                model="text-embedding-3-small",
                input=batch_texts,
            )
            batch_embeddings = [item.embedding for item in response.data]
            all_embeddings.extend(batch_embeddings)
            logger.info(f"Generated embeddings batch {i // batch_size + 1}: {len(all_embeddings)}/{len(texts)}")
        except Exception as e:
            logger.error(f"Failed to generate embeddings for batch starting at {i}: {e}")
            raise

    logger.info(f"Upserting {len(all_embeddings)} records to Supabase company_embeddings...")

    now_iso = datetime.now(timezone.utc).isoformat()
    records = []

    for profile, text, embedding in zip(profiles, texts, all_embeddings):
        records.append({
            "ticker_symbol": profile["ticker_symbol"],
            "exchange": profile["exchange"],
            "content_type": profile["content_type"],
            "content": text,
            "embedding": embedding,
            "metadata": {
                "icb_name2": profile["icb_name2"],
                "icb_name3": profile["icb_name3"],
                "icb_name4": profile["icb_name4"],
                "source": "vnstock_vci",
            },
            "updated_at": now_iso,
        })

    total_upserted = 0

    for i in range(0, len(records), batch_size):
        batch = records[i: i + batch_size]
        try:
            supabase.table("company_embeddings").upsert(
                batch, ignore_duplicates=False
            ).execute()
            total_upserted += len(batch)
            logger.info(
                f"Upserted batch {i // batch_size + 1}: {total_upserted}/{len(records)}"
            )
        except Exception as e:
            logger.error(f"Failed to upsert batch starting at {i}: {e}")
            raise

    logger.info(f"Successfully upserted {total_upserted} embeddings to Supabase")
    return total_upserted


def backfill_dim_assets_description(**context):
    """
    Task 4 (Bonus): Backfill empty description field in ClickHouse dim_assets.

    During the same DAG run, update tickers whose dim_assets.description is empty
    with the company_profile text fetched from vnstock.

    Uses ReplacingMergeTree-compatible insert with OPTIMIZE to deduplicate.
    """
    profiles = context["ti"].xcom_pull(key="profiles", task_ids="fetch_company_profiles")

    if not profiles:
        logger.info("No profiles available for backfill, skipping")
        return 0

    ch_client = get_clickhouse_client()

    query = """
    SELECT symbol
    FROM market_dwh.dim_assets
    WHERE asset_class = 'STOCK'
      AND market = 'VN'
      AND is_active = 1
      AND (description = '' OR description IS NULL)
    """

    result = ch_client.query(query)
    empty_desc_symbols = {row[0] for row in result.result_rows}

    logger.info(f"Found {len(empty_desc_symbols)} tickers with empty description in dim_assets")

    backfill_count = 0

    for profile in profiles:
        symbol = profile["ticker_symbol"]
        if symbol not in empty_desc_symbols:
            continue

        company_profile = profile["company_profile"]
        if not company_profile:
            continue

        try:
            ch_client.command(
                "ALTER TABLE market_dwh.dim_assets UPDATE description = {desc:String} "
                "WHERE symbol = {symbol:String} AND market = 'VN' AND asset_class = 'STOCK'",
                parameters={"desc": company_profile, "symbol": symbol},
            )
            backfill_count += 1
        except Exception as e:
            logger.warning(f"Failed to backfill description for {symbol}: {e}")

    if backfill_count > 0:
        try:
            ch_client.command("OPTIMIZE TABLE market_dwh.dim_assets FINAL")
        except Exception as e:
            logger.warning(f"OPTIMIZE TABLE failed (non-critical): {e}")

    logger.info(f"Backfilled description for {backfill_count} tickers in dim_assets")
    return backfill_count


task_fetch_profiles = PythonOperator(
    task_id="fetch_company_profiles",
    python_callable=fetch_company_profiles,
    dag=dag,
)

task_embed_and_upsert = PythonOperator(
    task_id="generate_and_upsert_embeddings",
    python_callable=generate_and_upsert_embeddings,
    dag=dag,
)

task_backfill = PythonOperator(
    task_id="backfill_dim_assets_description",
    python_callable=backfill_dim_assets_description,
    dag=dag,
)

task_fetch_profiles >> [task_embed_and_upsert, task_backfill]
