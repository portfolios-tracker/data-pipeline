"""
Company Intelligence Ingestion DAG
Story: 1-2-semantic-knowledge-ingestion-airflow-and-supabase

Fetches VN company profiles from the VCI provider, generates 768-dimensional
embeddings via Gemini text-embedding-004, and upserts to Supabase pgvector.

Schedule: Weekly Sunday 4 AM (after assets_dimension_etl at 2 AM)
"""

import logging
import os
import time
from datetime import datetime, timedelta, timezone
from itertools import islice
from typing import cast

from airflow import DAG
from airflow.sdk import task
from google import genai

try:
    from etl_modules.vci_provider import fetch_company_overview
except ModuleNotFoundError as exc:
    if exc.name != "etl_modules":
        raise
    from dags.etl_modules.vci_provider import fetch_company_overview

from supabase import Client, create_client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
VCI_GRAPHQL_POOL = "vci_graphql"
COMPANY_INTEL_CHUNK_SIZE = int(os.getenv("COMPANY_INTEL_CHUNK_SIZE", "50"))
EMBEDDING_BATCH_SIZE = int(os.getenv("COMPANY_INTEL_EMBEDDING_BATCH_SIZE", "100"))
PROFILE_FETCH_SLEEP_SECONDS = float(
    os.getenv("COMPANY_INTEL_PROFILE_FETCH_SLEEP_SECONDS", "0.5")
)


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

default_args = {
    "owner": "data-pipeline",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(  # type: ignore[call-arg]
    dag_id="ingest_company_intelligence",
    default_args=default_args,
    description="Ingest VN company profiles → Gemini embeddings → Supabase pgvector",
    schedule="0 4 * * 0",
    catchup=False,
    tags=["ai", "embeddings", "company", "supabase", "pgvector"],
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


def build_embedding_text(
    company_profile: str, icb_name2: str, icb_name3: str, icb_name4: str
) -> str:
    """
    Concatenate company profile text with sector taxonomy for embedding.

    Why: Embedding the sector context alongside the profile text improves
    semantic search quality for portfolio builder clustering.
    """
    sector_parts = [p for p in [icb_name2, icb_name3, icb_name4] if p and p.strip()]
    sector_str = " > ".join(sector_parts) if sector_parts else "Unknown"
    return f"{company_profile}\n\nSector: {sector_str}"


def _chunked_rows(rows, chunk_size):
    iterator = iter(rows)
    while True:
        chunk = list(islice(iterator, chunk_size))
        if not chunk:
            return
        yield chunk


def _load_active_vn_tickers() -> list[tuple[str, str]]:
    supabase = get_supabase_client()
    logger.info("Querying active VN tickers from Supabase market_data.assets...")
    response = (
        supabase.schema("market_data")
        .table("assets")
        .select("symbol,exchange,metadata")
        .eq("asset_class", "STOCK")
        .eq("market", "VN")
        .eq("status", "active")
        .order("symbol")
        .execute()
    )

    tickers = []
    rows = cast(list[dict], response.data or [])
    for row in rows:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        metadata = row.get("metadata") or {}
        symbol_type = str(metadata.get("symbol_type") or "").strip().upper()
        if symbol_type and symbol_type != "STOCK":
            continue
        tickers.append((symbol, str(row.get("exchange") or "")))

    logger.info("Found %s active VN tickers", len(tickers))
    return tickers


def fetch_company_profiles(**context):
    """
    Task 1: Query active VN tickers from Supabase market_data.assets and fetch company profiles via the VCI provider.

    - Rate limited: 0.5s sleep between API calls
    - Logs progress every 50 tickers
    - Skips tickers where vnstock returns no data
    """
    tickers = _load_active_vn_tickers()

    profiles = []

    for idx, (symbol, exchange) in enumerate(tickers):
        try:
            overview = fetch_company_overview(symbol)

            if overview is None or overview.empty:
                logger.warning(f"No overview data for {symbol}, skipping")
                time.sleep(PROFILE_FETCH_SLEEP_SECONDS)
                continue

            row = overview.iloc[0]
            company_profile = str(row.get("company_profile", "") or "").strip()
            icb_name2 = str(row.get("icb_name2", "") or "").strip()
            icb_name3 = str(row.get("icb_name3", "") or "").strip()
            icb_name4 = str(row.get("icb_name4", "") or "").strip()

            if not company_profile:
                logger.warning(f"Empty company_profile for {symbol}, skipping")
                time.sleep(PROFILE_FETCH_SLEEP_SECONDS)
                continue

            profiles.append(
                {
                    "ticker_symbol": symbol,
                    "exchange": exchange,
                    "content_type": "company_profile",
                    "company_profile": company_profile,
                    "icb_name2": icb_name2,
                    "icb_name3": icb_name3,
                    "icb_name4": icb_name4,
                }
            )

            if (idx + 1) % 50 == 0:
                logger.info(f"Processed {idx + 1}/{len(tickers)} tickers...")

        except Exception as e:
            logger.warning(f"Failed to fetch profile for {symbol}: {e}")

        time.sleep(PROFILE_FETCH_SLEEP_SECONDS)

    logger.info(f"Successfully fetched {len(profiles)} company profiles")

    context["ti"].xcom_push(key="profiles", value=profiles)
    return len(profiles)


def generate_and_upsert_embeddings(**context):
    """
    Task 2 + 3: Generate embeddings via Gemini text-embedding-004 and upsert to Supabase.

    - Batches embedding API calls in groups of 100
    - Upserts to Supabase company_embeddings in batches of 100
    - Includes ICB sector metadata in JSONB
    """
    profiles = context["ti"].xcom_pull(
        key="profiles", task_ids="fetch_company_profiles"
    )

    if not profiles:
        logger.warning("No profiles received from XCom, skipping embeddings")
        return 0

    client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))
    supabase = get_supabase_client()

    texts = [
        build_embedding_text(
            p["company_profile"], p["icb_name2"], p["icb_name3"], p["icb_name4"]
        )
        for p in profiles
    ]

    logger.info(
        "Generating embeddings for %s profiles in batches of %s...",
        len(texts),
        EMBEDDING_BATCH_SIZE,
    )

    all_embeddings = []
    batch_size = EMBEDDING_BATCH_SIZE

    for i in range(0, len(texts), batch_size):
        batch_texts = texts[i : i + batch_size]
        try:
            response = client.models.embed_content(
                model="text-embedding-004",
                contents=batch_texts,  # type: ignore[arg-type]
                config={"task_type": "CLUSTERING"},
            )
            embeddings = getattr(response, "embeddings", None) or []
            batch_embeddings = [ebd.values for ebd in embeddings]
            if len(batch_embeddings) != len(batch_texts):
                raise ValueError(
                    "Embedding count mismatch for batch "
                    f"{i // batch_size + 1}: expected {len(batch_texts)}, got {len(batch_embeddings)}"
                )
            all_embeddings.extend(batch_embeddings)
            logger.info(
                f"Generated embeddings batch {i // batch_size + 1}: {len(all_embeddings)}/{len(texts)}"
            )
        except Exception as e:
            logger.error(
                f"Failed to generate embeddings for batch starting at {i}: {e}"
            )
            raise

    logger.info(
        f"Upserting {len(all_embeddings)} records to Supabase company_embeddings..."
    )
    if len(all_embeddings) != len(texts):
        raise ValueError(
            "Embedding count mismatch before upsert: "
            f"expected {len(texts)}, got {len(all_embeddings)}"
        )

    now_iso = datetime.now(timezone.utc).isoformat()
    records = []

    for profile, text, embedding in zip(profiles, texts, all_embeddings):
        records.append(
            {
                "ticker_symbol": profile["ticker_symbol"],
                "exchange": profile["exchange"],
                "content_type": profile["content_type"],
                "content": text,
                "embedding": embedding,
                "metadata": {
                    "icb_name2": profile["icb_name2"],
                    "icb_name3": profile["icb_name3"],
                    "icb_name4": profile["icb_name4"],
                    "source": "vnstock_vci_gemini",
                },
                "updated_at": now_iso,
            }
        )

    total_upserted = 0

    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]
        try:
            supabase.table("market_data.company_embeddings").upsert(
                batch, ignore_duplicates=False, on_conflict="ticker_symbol,content_type"
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
    Legacy task retained for DAG compatibility.

    The Supabase ``assets`` table does not store a ``description`` column, so
    there is no equivalent backfill action after ClickHouse retirement.
    """
    logger.info(
        "Skipping legacy description backfill: no description column in Supabase assets"
    )
    return 0


@task(show_return_value_in_logs=False)
def list_company_tickers():
    tickers = _load_active_vn_tickers()
    return [{"symbol": symbol, "exchange": exchange} for symbol, exchange in tickers]


@task(show_return_value_in_logs=False)
def chunk_company_tickers(tickers):
    chunks = []
    for chunk_index, ticker_chunk in enumerate(
        _chunked_rows(tickers, COMPANY_INTEL_CHUNK_SIZE), start=1
    ):
        chunks.append({"chunk_index": chunk_index, "tickers": ticker_chunk})

    logger.info(
        "Prepared %s company intelligence chunks (chunk_size=%s)",
        len(chunks),
        COMPANY_INTEL_CHUNK_SIZE,
    )
    return chunks


@task
def process_company_intelligence_chunk(chunk_payload):
    chunk_index = int(chunk_payload.get("chunk_index") or 0)
    ticker_rows = chunk_payload.get("tickers") or []
    chunk_tickers = [
        (str(item.get("symbol") or "").strip().upper(), str(item.get("exchange") or ""))
        for item in ticker_rows
        if str(item.get("symbol") or "").strip()
    ]

    failed_tickers = []
    failed_embedding_batches = []
    failed_upsert_batches = []
    fatal_error = None

    try:
        gemini_key = os.getenv("GEMINI_API_KEY")
        if not gemini_key:
            raise ValueError("GEMINI_API_KEY environment variable not set")
        client = genai.Client(api_key=gemini_key)
        supabase = get_supabase_client()
    except Exception as exc:
        fatal_error = str(exc)
        logger.error(
            "process_company_intelligence_chunk[%s] setup failed: %s",
            chunk_index,
            exc,
        )
        return {
            "chunk_index": chunk_index,
            "chunk_tickers": len(chunk_tickers),
            "profiles_fetched": 0,
            "embeddings_generated": 0,
            "rows_upserted": 0,
            "failed_tickers": failed_tickers,
            "failed_embedding_batches": failed_embedding_batches,
            "failed_upsert_batches": failed_upsert_batches,
            "fatal_error": fatal_error,
        }

    profiles = []
    for symbol, exchange in chunk_tickers:
        try:
            overview = fetch_company_overview(symbol)
            if overview is None or overview.empty:
                failed_tickers.append({"symbol": symbol, "error": "empty overview"})
                time.sleep(PROFILE_FETCH_SLEEP_SECONDS)
                continue

            row = overview.iloc[0]
            company_profile = str(row.get("company_profile", "") or "").strip()
            if not company_profile:
                failed_tickers.append(
                    {"symbol": symbol, "error": "empty company_profile"}
                )
                time.sleep(PROFILE_FETCH_SLEEP_SECONDS)
                continue

            profiles.append(
                {
                    "ticker_symbol": symbol,
                    "exchange": exchange,
                    "content_type": "company_profile",
                    "company_profile": company_profile,
                    "icb_name2": str(row.get("icb_name2", "") or "").strip(),
                    "icb_name3": str(row.get("icb_name3", "") or "").strip(),
                    "icb_name4": str(row.get("icb_name4", "") or "").strip(),
                }
            )
        except Exception as exc:
            failed_tickers.append({"symbol": symbol, "error": str(exc)})

        time.sleep(PROFILE_FETCH_SLEEP_SECONDS)

    embedded_payloads = []
    for offset in range(0, len(profiles), EMBEDDING_BATCH_SIZE):
        profile_batch = profiles[offset : offset + EMBEDDING_BATCH_SIZE]
        text_batch = [
            build_embedding_text(
                profile["company_profile"],
                profile["icb_name2"],
                profile["icb_name3"],
                profile["icb_name4"],
            )
            for profile in profile_batch
        ]

        try:
            response = client.models.embed_content(
                model="text-embedding-004",
                contents=text_batch,  # type: ignore[arg-type]
                config={"task_type": "CLUSTERING"},
            )
            embeddings = getattr(response, "embeddings", None) or []
            values = [embedding.values for embedding in embeddings]
            if len(values) != len(profile_batch):
                raise ValueError(
                    "Embedding count mismatch for chunk "
                    f"{chunk_index} batch {offset // EMBEDDING_BATCH_SIZE + 1}: "
                    f"expected {len(profile_batch)}, got {len(values)}"
                )
            embedded_payloads.extend(
                [
                    {
                        "profile": profile,
                        "text": text,
                        "embedding": embedding_values,
                    }
                    for profile, text, embedding_values in zip(
                        profile_batch, text_batch, values
                    )
                ]
            )
        except Exception as exc:
            failed_embedding_batches.append(
                {
                    "batch_index": offset // EMBEDDING_BATCH_SIZE + 1,
                    "size": len(profile_batch),
                    "error": str(exc),
                }
            )

    now_iso = datetime.now(timezone.utc).isoformat()
    records = []
    for payload in embedded_payloads:
        profile = payload["profile"]
        records.append(
            {
                "ticker_symbol": profile["ticker_symbol"],
                "exchange": profile["exchange"],
                "content_type": profile["content_type"],
                "content": payload["text"],
                "embedding": payload["embedding"],
                "metadata": {
                    "icb_name2": profile["icb_name2"],
                    "icb_name3": profile["icb_name3"],
                    "icb_name4": profile["icb_name4"],
                    "source": "vnstock_vci_gemini",
                },
                "updated_at": now_iso,
            }
        )

    total_upserted = 0
    for offset in range(0, len(records), EMBEDDING_BATCH_SIZE):
        batch = records[offset : offset + EMBEDDING_BATCH_SIZE]
        try:
            supabase.table("market_data.company_embeddings").upsert(
                batch,
                ignore_duplicates=False,
                on_conflict="ticker_symbol,content_type",
            ).execute()
            total_upserted += len(batch)
        except Exception as exc:
            failed_upsert_batches.append(
                {
                    "batch_index": offset // EMBEDDING_BATCH_SIZE + 1,
                    "size": len(batch),
                    "error": str(exc),
                }
            )

    summary = {
        "chunk_index": chunk_index,
        "chunk_tickers": len(chunk_tickers),
        "profiles_fetched": len(profiles),
        "embeddings_generated": len(embedded_payloads),
        "rows_upserted": total_upserted,
        "failed_tickers": failed_tickers,
        "failed_embedding_batches": failed_embedding_batches,
        "failed_upsert_batches": failed_upsert_batches,
        "fatal_error": fatal_error,
    }
    logger.info(
        "process_company_intelligence_chunk summary: chunk=%s tickers=%s "
        "profiles=%s embeddings=%s rows_upserted=%s failed_tickers=%s "
        "failed_embedding_batches=%s failed_upsert_batches=%s fatal_error=%s",
        summary["chunk_index"],
        summary["chunk_tickers"],
        summary["profiles_fetched"],
        summary["embeddings_generated"],
        summary["rows_upserted"],
        len(failed_tickers),
        len(failed_embedding_batches),
        len(failed_upsert_batches),
        bool(fatal_error),
    )
    return summary


@task(trigger_rule="all_done")
def finalize_company_intelligence(chunk_results):
    results = [result for result in (chunk_results or []) if isinstance(result, dict)]
    if not results:
        raise RuntimeError("No chunk results were produced for company intelligence")

    totals = {
        "chunks": len(results),
        "tickers": sum(int(result.get("chunk_tickers") or 0) for result in results),
        "profiles": sum(int(result.get("profiles_fetched") or 0) for result in results),
        "embeddings": sum(
            int(result.get("embeddings_generated") or 0) for result in results
        ),
        "rows_upserted": sum(
            int(result.get("rows_upserted") or 0) for result in results
        ),
    }

    failed_tickers = []
    failed_embedding_batches = []
    failed_upsert_batches = []
    fatal_errors = []
    for result in results:
        failed_tickers.extend(result.get("failed_tickers") or [])
        failed_embedding_batches.extend(result.get("failed_embedding_batches") or [])
        failed_upsert_batches.extend(result.get("failed_upsert_batches") or [])
        fatal_error = result.get("fatal_error")
        if fatal_error:
            fatal_errors.append(
                {
                    "chunk_index": result.get("chunk_index"),
                    "error": str(fatal_error),
                }
            )

    logger.info(
        "company intelligence summary: chunks=%s tickers=%s profiles=%s "
        "embeddings=%s rows_upserted=%s failed_tickers=%s "
        "failed_embedding_batches=%s failed_upsert_batches=%s fatal_errors=%s",
        totals["chunks"],
        totals["tickers"],
        totals["profiles"],
        totals["embeddings"],
        totals["rows_upserted"],
        len(failed_tickers),
        len(failed_embedding_batches),
        len(failed_upsert_batches),
        len(fatal_errors),
    )

    if fatal_errors:
        raise RuntimeError(
            "Company intelligence pipeline completed with fatal chunk errors: "
            f"fatal_errors={len(fatal_errors)}"
        )

    alert_mode = bool(
        failed_tickers or failed_embedding_batches or failed_upsert_batches
    )
    if alert_mode:
        logger.warning(
            "company intelligence alert mode: partial failures detected but DAG "
            "will complete successfully. failed_tickers=%s "
            "failed_embedding_batches=%s failed_upsert_batches=%s",
            len(failed_tickers),
            len(failed_embedding_batches),
            len(failed_upsert_batches),
        )

    return {
        **totals,
        "alert_mode": alert_mode,
        "failed_tickers": len(failed_tickers),
        "failed_embedding_batches": len(failed_embedding_batches),
        "failed_upsert_batches": len(failed_upsert_batches),
    }


@task
def run_backfill_placeholder():
    return backfill_dim_assets_description()


with dag:
    company_tickers = list_company_tickers()
    company_chunks = chunk_company_tickers(company_tickers)
    chunk_results = process_company_intelligence_chunk.override(
        pool=VCI_GRAPHQL_POOL
    ).expand(chunk_payload=company_chunks)
    company_summary = finalize_company_intelligence(chunk_results)
    backfill_result = run_backfill_placeholder()

    _ = (
        company_tickers
        >> company_chunks
        >> chunk_results
        >> company_summary
        >> backfill_result
    )
