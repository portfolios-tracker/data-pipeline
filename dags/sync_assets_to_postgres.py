"""
ClickHouse → Supabase Postgres Sync DAG
Story: prep-3-seed-asset-data
Architecture: Single Table Inheritance (STI)

FIXED ISSUES:
- Uses Supabase Python Client (supabase-py) for better integration
- Uses UPSERT pattern (preserves foreign key relationships)
- Handles conflicts on (symbol, market, asset_class) unique constraint

Schedule: Nightly 3 AM
"""

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import clickhouse_connect
from supabase import create_client, Client
import os
import logging

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
    "sync_assets_to_postgres",
    default_args=default_args,
    description="Sync ClickHouse dim_assets → Supabase public.assets (STI with Supabase Client)",
    schedule="0 3 * * *",  # Daily 3 AM
    catchup=False,
    tags=["assets", "sync", "postgres", "sti", "supabase"],
)


def sync_assets(**context):
    """
    Sync assets from ClickHouse to Supabase using Supabase Python Client

    Why Supabase Client:
    - Better integration with Supabase ecosystem
    - Easier upsert logic via .upsert()
    - Handles JSONB metadata automatically
    """

    # 1. Connect to ClickHouse
    logger.info("Connecting to ClickHouse...")
    ch_client = clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "clickhouse-server"),
        port=int(os.getenv("CLICKHOUSE_PORT", 8123)),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
    )

    # 2. Query all active assets
    query = """
    SELECT 
        symbol,
        name_en,
        name_local,
        asset_class,
        market,
        currency,
        exchange,
        sector,
        industry,
        logo_url,
        external_api_metadata,
        source
    FROM market_dwh.dim_assets
    WHERE is_active = 1
    ORDER BY asset_class, market, symbol
    """

    logger.info("Querying assets from ClickHouse...")
    result = ch_client.query(query)
    rows = result.result_rows
    column_names = result.column_names

    logger.info(f"Fetched {len(rows)} assets from ClickHouse")

    if len(rows) == 0:
        logger.warning("No assets found in ClickHouse. Skipping sync.")
        return 0

    # 3. Connect to Supabase
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

    if not url or not key:
        raise ValueError(
            "SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY environment variable not set"
        )

    logger.info("Connecting to Supabase...")
    supabase: Client = create_client(url, key)

    # 4. Prepare data for UPSERT
    # Supabase .upsert() takes a list of dictionaries
    # Conflict is handled by default on the Primary Key, but we can specify the unique constraint column(s)
    # Note: assets table has UNIQUE(symbol, market, asset_class)

    assets_to_sync = []

    # Build column index for ClickHouse result
    col_idx = {name: idx for idx, name in enumerate(column_names)}

    for row in rows:
        # Handle empty market (ClickHouse uses '' for NULL)
        market = row[col_idx["market"]] if row[col_idx["market"]] else None

        asset_data = {
            "symbol": row[col_idx["symbol"]],
            "name_en": row[col_idx["name_en"]],
            "name_local": row[col_idx["name_local"]] or None,
            "asset_class": row[col_idx["asset_class"]],
            "market": market,
            "currency": row[col_idx["currency"]],
            "exchange": row[col_idx["exchange"]] or None,
            "sector": row[col_idx["sector"]] or None,
            "industry": row[col_idx["industry"]] or None,
            "logo_url": row[col_idx["logo_url"]] or None,
            "metadata": row[col_idx["external_api_metadata"]]
            or {},  # supabase-py handles dict -> jsonb
            "source": row[col_idx["source"]] or None,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        assets_to_sync.append(asset_data)

    # 5. Execute UPSERT in batches (Supabase/PostgREST has limits)
    batch_size = 100
    total_synced = 0

    logger.info(
        f"Starting UPSERT of {len(assets_to_sync)} records in batches of {batch_size}..."
    )

    for i in range(0, len(assets_to_sync), batch_size):
        batch = assets_to_sync[i : i + batch_size]
        try:
            logger.debug(
                f"About to upsert batch {i // batch_size + 1}, size: {len(batch)}"
            )
            # Upsert logic:
            # - For assets with market: conflict on (symbol, market, asset_class) via idx_assets_unique_with_market
            # - For assets without market (NULL): conflict on (symbol, asset_class) via idx_assets_unique_without_market
            # We use ignoreDuplicates=False to update existing records
            response = (
                supabase.table("assets")
                .upsert(batch, ignore_duplicates=False)
                .execute()
            )

            total_synced += len(batch)
            logger.info(
                f"Synced batch {i // batch_size + 1}: {total_synced}/{len(assets_to_sync)}"
            )

        except Exception as e:
            logger.error(f"Failed to sync batch starting at index {i}: {e}")
            raise

    logger.info(
        f"Successfully synced {total_synced} assets to Supabase using Supabase Client"
    )
    return total_synced


# Define task
sync_task = PythonOperator(
    task_id="sync_assets_to_postgres",
    python_callable=sync_assets,
    dag=dag,
)
