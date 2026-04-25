import json
import logging
import os
import re
import time
from datetime import datetime, timedelta

import pandas as pd
import psycopg2
import psycopg2.extras
from airflow import DAG
from airflow.sdk import task
from pendulum import timezone

from dags.etl_modules.extractors import run_all_extractors
from dags.etl_modules.notifications import (
    send_failure_notification,
    send_success_notification,
)

logger = logging.getLogger(__name__)

SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")

def truncate_text(text: str, max_chars: int = 1000) -> str:
    """Truncates text to a maximum number of characters and appends '...' if truncated."""
    if not text:
        return ""
    if len(text) > max_chars:
        return text[:max_chars] + "..."
    return text

default_args = {
    "owner": "data_engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

local_tz = timezone("Asia/Bangkok")

with DAG(
    dag_id="market_news_morning",
    default_args=default_args,
    schedule="0 7 * * 1-5",  # 7 AM Vietnam Time Mon-Fri
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),
    catchup=False,
    tags=["news", "supabase", "morning-brief"],
    on_success_callback=send_success_notification,
    on_failure_callback=send_failure_notification,
) as dag:

    @task
    def extract_and_load_news() -> None:
        """
        Extract news from all sources and load them directly into the database
        to avoid large XCom payloads.
        """
        data = run_all_extractors()
        logger.info(
            "extract_and_load_news: %s records extracted from all sources", len(data)
        )

        if not data:
            logger.info("No news to load.")
            return

        if not SUPABASE_DB_URL:
            raise RuntimeError("SUPABASE_DB_URL environment variable is not set")

        cols = [
            "asset_id",
            "news_id",
            "publish_date",
            "title",
            "news_content",
            "source",
            "source_url",
        ]
        tuples = []
        for row in data:
            if row.get("publish_date"):
                row["publish_date"] = pd.to_datetime(row["publish_date"])
            tuples.append([row.get(c) for c in cols])

        logger.info(
            "extract_and_load_news: inserting %s rows into market_data.news",
            len(tuples),
        )
        conn = psycopg2.connect(SUPABASE_DB_URL)
        try:
            with conn:
                with conn.cursor() as cur:
                    psycopg2.extras.execute_values(
                        cur,
                        """
                        INSERT INTO market_data.news
                            (asset_id, news_id, publish_date, title,
                             news_content, source, source_url)
                        VALUES %s
                        ON CONFLICT (asset_id, news_id) DO UPDATE SET
                            title           = EXCLUDED.title,
                            news_content    = EXCLUDED.news_content,
                            ingested_at     = NOW()
                        """,
                        tuples,
                    )
        finally:
            conn.close()

        logger.info("extract_and_load_news: complete")

    @task(pool="gemini_api_pool")
    def score_news() -> None:
        """Add sentiment score to unscored news records using Gemini online API (Tier 1 limits)."""
        import time
        from google import genai
        from google.genai import errors, types

        task_logger = logging.getLogger("airflow.task")

        if not SUPABASE_DB_URL:
            raise RuntimeError("SUPABASE_DB_URL environment variable is not set")

        conn = psycopg2.connect(SUPABASE_DB_URL)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT public.fetch_secret_by_name('GEMINI_API_KEY')")
                row = cur.fetchone()
                google_api_key = row[0] if row else None
        finally:
            conn.close()

        if not google_api_key:
            task_logger.warning(
                "GEMINI_API_KEY not found in Vault. Skipping sentiment scoring."
            )
            return

        # Read unscored news, deduplicating by news_id
        conn = psycopg2.connect(SUPABASE_DB_URL)
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT news_id, max(title) as title, max(news_content) as description
                    FROM market_data.news
                    WHERE sentiment_score IS NULL
                    GROUP BY news_id
                    """
                )
                data = cur.fetchall()
        finally:
            conn.close()

        if not data:
            task_logger.info("No unscored news found.")
            return

        try:
            client = genai.Client(api_key=google_api_key)
        except Exception as e:
            task_logger.error(f"Failed to initialize Gemini client: {e}")
            raise

        inline_requests = []
        for item in data:
            title = item.get("title", "No Title")
            raw_desc = item.get("description", "")
            desc = truncate_text(raw_desc, 1000)
            
            prompt = f"Analyze sentiment for the following financial news item from the Vietnamese stock market. Return a single numeric score between -1.0 (extremely negative) and 1.0 (extremely positive). 0.0 is neutral. Just return the number, nothing else.\n\nTitle: {title}\nContent: {desc}"
            
            inline_requests.append({
                "contents": [{"parts": [{"text": prompt}]}],
            })

        task_logger.info("Submitting batch job for %s items...", len(data))
        
        batch_job = client.batches.create(
            model="gemini-2.0-flash",
            src=inline_requests
        )
        task_logger.info("Batch job %s submitted.", batch_job.name)

        completed_states = {'JOB_STATE_SUCCEEDED', 'JOB_STATE_FAILED', 'JOB_STATE_CANCELLED', 'JOB_STATE_PAUSED'}
        while batch_job.state.name not in completed_states:
            time.sleep(30)
            batch_job = client.batches.get(name=batch_job.name)
            task_logger.info("Batch job status: %s", batch_job.state.name)

        if batch_job.state.name != 'JOB_STATE_SUCCEEDED':
            raise RuntimeError(f"Batch job failed: {batch_job.error}")

        batch_updates = []
        if batch_job.dest.inlined_responses:
            for idx, resp in enumerate(batch_job.dest.inlined_responses):
                try:
                    response_text = resp.response.candidates[0].content.parts[0].text
                    import re as regex
                    match = regex.search(r'-?\d+\.\d+|-?\d+', response_text)
                    score = float(match.group()) if match else 0.0
                    sentiment_score = max(-1.0, min(1.0, score))
                    news_id = data[idx]['news_id']
                    batch_updates.append((sentiment_score, news_id))
                except Exception as e:
                    task_logger.error(f"Failed to parse inline response {idx}: {e}")
                    batch_updates.append((0.0, data[idx]['news_id']))
        else:
            task_logger.warning("No inlined_responses found in job destination.")
            
        if batch_updates:
            conn = psycopg2.connect(SUPABASE_DB_URL)
            try:
                with conn:
                    with conn.cursor() as cur:
                        psycopg2.extras.execute_values(
                            cur,
                            """
                            UPDATE market_data.news AS n
                            SET sentiment_score = v.sentiment_score
                            FROM (VALUES %s) AS v(sentiment_score, news_id)
                            WHERE n.news_id = v.news_id
                            """,
                            batch_updates,
                        )
            finally:
                conn.close()
            task_logger.info("Saved sentiment scores for %s news items.", len(batch_updates))

    @task(pool="gemini_api_pool")
    def embed_news() -> None:
        """
        Embed today's new articles with gemini-embedding-001 @ 768d.
        Reads from the DB directly — no XCom dependency.
        API key is retrieved from Supabase Vault via fetch_secret_by_name().
        """
        import time
        from datetime import date

        import numpy as np
        from google import genai
        from google.genai import errors, types

        task_logger = logging.getLogger("airflow.task")

        if not SUPABASE_DB_URL:
            raise RuntimeError("SUPABASE_DB_URL environment variable is not set")

        # ── Retrieve GEMINI_API_KEY from Supabase Vault ───────────────────────
        conn = psycopg2.connect(SUPABASE_DB_URL)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT public.fetch_secret_by_name('GEMINI_API_KEY')")
                row = cur.fetchone()
                gemini_api_key = row[0] if row else None
        finally:
            conn.close()

        if not gemini_api_key:
            raise RuntimeError(
                "GEMINI_API_KEY not found in Supabase Vault — "
                "add the secret via the Vault UI before running this task."
            )

        client = genai.Client(api_key=gemini_api_key)
        MODEL = "gemini-embedding-001"
        DIM = 768
        BATCH = 100

        # ── Fetch today's unembedded articles ─────────────────────────────────
        conn = psycopg2.connect(SUPABASE_DB_URL)
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT n.asset_id, n.news_id, n.title, n.news_content
                    FROM market_data.news n
                    LEFT JOIN market_data.news_embeddings e
                        ON n.asset_id = e.asset_id
                        AND n.news_id = e.news_id
                    WHERE n.publish_date::date = %s
                      AND e.news_id IS NULL
                    """,
                    (date.today(),),
                )
                rows = cur.fetchall()
        finally:
            conn.close()

        if not rows:
            task_logger.info("embed_news: no new articles to embed today")
            return

        task_logger.info(
            "embed_news: embedding %s articles with %s @ %dd (Tier 1: High RPM limit)",
            len(rows),
            MODEL,
            DIM,
        )

        def _normalize(vec: list[float]) -> list[float]:
            arr = np.array(vec, dtype=np.float32)
            norm = np.linalg.norm(arr)
            return (arr / norm if norm > 0 else arr).tolist()

        max_retries = 3

        # ── Batch embedding with Gemini Batch API ───────────────────────────────
        texts = [
            f"{r['title']}. {truncate_text(r['news_content'] or '', 3000)}".strip() for r in rows
        ]

        task_logger.info("Submitting embeddings batch job for %s articles...", len(texts))
        
        batch_job = client.batches.create_embeddings(
            model=MODEL,
            src=texts
        )
        task_logger.info("Batch job %s submitted.", batch_job.name)

        completed_states = {'JOB_STATE_SUCCEEDED', 'JOB_STATE_FAILED', 'JOB_STATE_CANCELLED', 'JOB_STATE_PAUSED'}
        while batch_job.state.name not in completed_states:
            time.sleep(30)
            batch_job = client.batches.get(name=batch_job.name)
            task_logger.info("Batch job status: %s", batch_job.state.name)

        if batch_job.state.name != 'JOB_STATE_SUCCEEDED':
            raise RuntimeError(f"Batch job failed: {batch_job.error}")

        tuples = []
        if batch_job.dest.inlined_embed_content_responses:
            for idx, resp in enumerate(batch_job.dest.inlined_embed_content_responses):
                try:
                    values = resp.response.embeddings[0].values
                    if not values:
                        continue
                    norm_values = _normalize(values)
                    tuples.append((
                        str(rows[idx]["asset_id"]),
                        int(rows[idx]["news_id"]),
                        norm_values,
                        "gemini-embedding-001-768d",
                    ))
                except Exception as e:
                    task_logger.error(f"Failed to parse embedding inline response {idx}: {e}")
        else:
            task_logger.warning("No inlined_embed_content_responses found in job destination.")
            
        if tuples:
            conn = psycopg2.connect(SUPABASE_DB_URL)
            try:
                with conn:
                    with conn.cursor() as cur:
                        psycopg2.extras.execute_values(
                            cur,
                            """
                            INSERT INTO market_data.news_embeddings
                                (asset_id, news_id, embedding, model_ver)
                            VALUES %s
                            ON CONFLICT (asset_id, news_id) DO UPDATE SET
                                embedding   = EXCLUDED.embedding,
                                model_ver   = EXCLUDED.model_ver,
                                embedded_at = NOW()
                            """,
                            tuples,
                        )
            finally:
                conn.close()
            task_logger.info("Saved %s embeddings.", len(tuples))

    # ── DAG orchestration ─────────────────────────────────────────────────────
    extract = extract_and_load_news()
    score = score_news()
    embed = embed_news()

    extract >> score >> embed
