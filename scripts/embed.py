"""
Sync-threaded direct embedding test script — NOT a DAG.
Embeds unprocessed news articles via Gemini API using a thread pool and upserts to DB.

Usage:
    SUPABASE_DB_URL=... uv run scripts/embed.py [--limit 50] [--concurrency 10]
"""

import argparse
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from threading import Lock

import numpy as np
import psycopg2
import psycopg2.extras
from google import genai

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dags.etl_modules.gemini_helpers import (
    SUPABASE_DB_URL,
    chunk_text,
    get_gemini_api_key,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

MODEL = "models/gemini-embedding-001"
TARGET_DIM = 768


@dataclass
class ChunkJob:
    news_row_id: str
    chunk_index: int
    text: str


def _normalize(vec: list[float]) -> list[float]:
    arr = np.array(vec, dtype=np.float32)
    norm = np.linalg.norm(arr)
    return (arr / norm if norm > 0 else arr).tolist()


def fetch_unembedded_articles(limit: int) -> list[dict]:
    conn = psycopg2.connect(SUPABASE_DB_URL)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT n.id AS news_row_id, n.title, n.news_content
                FROM market_data.news n
                WHERE NOT EXISTS (
                    SELECT 1 FROM market_data.news_embeddings e
                    WHERE e.news_row_id = n.id
                )
                  AND n.news_content IS NOT NULL
                  AND trim(n.news_content) != ''
                LIMIT %s
                """,
                (limit,),
            )
            return cur.fetchall()
    finally:
        conn.close()


def build_chunk_jobs(rows: list[dict]) -> list[ChunkJob]:
    jobs = []
    for r in rows:
        full_text = f"{r['title']}. {r['news_content'] or ''}".strip()
        chunks = chunk_text(full_text, chunk_size=4000, chunk_overlap=800)
        for i, chunk in enumerate(chunks):
            jobs.append(
                ChunkJob(
                    news_row_id=str(r["news_row_id"]),
                    chunk_index=i,
                    text=chunk,
                )
            )
    return jobs


def upsert_embeddings(tuples: list[tuple]) -> None:
    conn = psycopg2.connect(SUPABASE_DB_URL)
    try:
        with conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO market_data.news_embeddings
                        (news_row_id, chunk_index, embedding, model_ver)
                    VALUES %s
                    ON CONFLICT (news_row_id, chunk_index) DO UPDATE SET
                        embedding   = EXCLUDED.embedding,
                        model_ver   = EXCLUDED.model_ver,
                        embedded_at = NOW()
                    """,
                    tuples,
                )
    finally:
        conn.close()


def embed_chunk(
    client: genai.Client,
    job: ChunkJob,
    stats: dict,
    stats_lock: Lock,
) -> tuple | None:
    """Called in a thread. One shared client is thread-safe for HTTP requests."""
    try:
        response = client.models.embed_content(
            model=MODEL,
            contents=job.text,
            config={
                "output_dimensionality": TARGET_DIM,
                "task_type": "RETRIEVAL_DOCUMENT",
            },
        )
        values = response.embeddings[0].values
        if not values:
            logger.warning(f"Empty embedding: {job.news_row_id}[{job.chunk_index}]")
            with stats_lock:
                stats["skipped"] += 1
            return None

        with stats_lock:
            stats["success"] += 1
        return (
            job.news_row_id,
            job.chunk_index,
            _normalize(values),
            f"gemini-001-{TARGET_DIM}d",
        )
    except Exception as e:
        logger.error(f"Failed {job.news_row_id}[{job.chunk_index}]: {e}")
        with stats_lock:
            stats["failed"] += 1
        return None


def run(limit: int, concurrency: int) -> None:
    api_key = get_gemini_api_key()
    if not api_key:
        raise RuntimeError("Could not retrieve GEMINI_API_KEY from DB.")

    logger.info(f"Fetching up to {limit} unembedded articles...")
    rows = fetch_unembedded_articles(limit)
    if not rows:
        logger.info("Nothing to embed.")
        return

    jobs = build_chunk_jobs(rows)
    logger.info(
        f"Articles: {len(rows)} -> Chunks: {len(jobs)} (concurrency={concurrency})"
    )

    client = genai.Client(api_key=api_key)
    stats = {"success": 0, "failed": 0, "skipped": 0}
    stats_lock = Lock()
    tuples = []

    t0 = time.perf_counter()

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = {
            executor.submit(embed_chunk, client, job, stats, stats_lock): job
            for job in jobs
        }
        for future in as_completed(futures):
            result = future.result()
            if result:
                tuples.append(result)

    elapsed = time.perf_counter() - t0
    logger.info(
        f"Embedding done in {elapsed:.1f}s - "
        f"success={stats['success']}, failed={stats['failed']}, skipped={stats['skipped']}"
    )

    if tuples:
        logger.info(f"Upserting {len(tuples)} embeddings to DB...")
        upsert_embeddings(tuples)
        logger.info("Upsert complete.")
    else:
        logger.warning("No embeddings to upsert.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync-threaded Gemini embedding test")
    parser.add_argument("--limit", type=int, default=50, help="Max articles to process")
    parser.add_argument("--concurrency", type=int, default=10, help="Thread pool size")
    args = parser.parse_args()

    run(limit=args.limit, concurrency=args.concurrency)
