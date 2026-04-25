import json
import logging
from datetime import timedelta

import numpy as np
import psycopg2
import psycopg2.extras
from airflow import DAG
from airflow.sdk import task
from airflow.sdk.bases.sensor import PokeReturnValue
from pendulum import timezone

from dags.etl_modules.gemini_helpers import (
    SUPABASE_DB_URL,
    get_gemini_api_key,
    truncate_text,
    chunk_text,
)

logger = logging.getLogger(__name__)

default_args = {
    "owner": "data_engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# gemini-embedding-2 is the latest model and supports Matryoshka embeddings.
# It returns 3072 dimensions by default, but we slice it to 768 for HNSW indexing performance.
MODEL = "models/gemini-embedding-2"
TARGET_DIM = 768


def _normalize(vec: list[float]) -> list[float]:
    arr = np.array(vec, dtype=np.float32)
    norm = np.linalg.norm(arr)
    return (arr / norm if norm > 0 else arr).tolist()


with DAG(
    dag_id="market_news_embedding",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["news", "embedding", "gemini-batch"],
) as dag:

    @task
    def submit_embed_batch() -> str | None:
        from google import genai

        api_key = get_gemini_api_key()
        if not api_key:
            return None

        conn = psycopg2.connect(SUPABASE_DB_URL)
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT n.asset_id, n.news_id, n.title, n.news_content
                    FROM market_data.news n
                    WHERE NOT EXISTS (
                        SELECT 1 FROM market_data.news_embeddings e
                        WHERE e.asset_id = n.asset_id AND e.news_id = n.news_id
                    )
                    LIMIT 200
                    """
                )
                rows = cur.fetchall()
        finally:
            conn.close()

        if not rows:
            return None

        texts_to_embed = []
        id_mappings = []

        for r in rows:
            full_text = f"{r['title']}. {r['news_content'] or ''}".strip()
            # 4000 chars is ~800 words, safely under the 2048 token limit
            chunks = chunk_text(full_text, chunk_size=4000, chunk_overlap=500)

            for i, chunk in enumerate(chunks):
                texts_to_embed.append(chunk)
                id_mappings.append(
                    {
                        "asset_id": str(r["asset_id"]),
                        "news_id": int(r["news_id"]),
                        "chunk_index": i,
                    }
                )

        client = genai.Client(api_key=api_key)

        # Batch create using the high-fidelity embedding-2 model
        batch_job = client.batches.create_embeddings(
            model=MODEL, src={"inlined_requests": {"contents": texts_to_embed}}
        )
        return json.dumps({"job_name": batch_job.name, "mappings": id_mappings})

    @task.sensor(poke_interval=60, timeout=3600, mode="reschedule")
    def wait_for_embed_batch(job_payload: str | None) -> PokeReturnValue:
        if not job_payload:
            return PokeReturnValue(is_done=True, xcom_value=None)

        payload = json.loads(job_payload)
        job_name = payload["job_name"]

        from google import genai

        client = genai.Client(api_key=get_gemini_api_key())
        batch_job = client.batches.get(name=job_name)

        if batch_job.state.name == "JOB_STATE_SUCCEEDED":
            return PokeReturnValue(is_done=True, xcom_value=json.dumps(payload))
        elif batch_job.state.name in ["JOB_STATE_FAILED", "JOB_STATE_CANCELLED"]:
            raise RuntimeError(f"Batch job failed: {batch_job.error}")

        return PokeReturnValue(is_done=False)

    @task
    def process_embed_batch(job_payload: str | None) -> None:
        if not job_payload:
            return

        payload = json.loads(job_payload)
        job_name = payload["job_name"]
        mappings = payload["mappings"]

        from google import genai

        client = genai.Client(api_key=get_gemini_api_key())
        batch_job = client.batches.get(name=job_name)

        tuples = []
        if batch_job.dest.inlined_embed_content_responses:
            for idx, resp in enumerate(batch_job.dest.inlined_embed_content_responses):
                try:
                    # SingleEmbedContentResponse has a single 'embedding' attribute
                    full_values = resp.response.embedding.values
                    if not full_values:
                        continue

                    # Slice to TARGET_DIM (768) and re-normalize.
                    # Matryoshka embeddings are designed to be sliced like this.
                    sliced_values = full_values[:TARGET_DIM]
                    norm_values = _normalize(sliced_values)

                    m = mappings[idx]
                    tuples.append(
                        (
                            m["asset_id"],
                            m["news_id"],
                            m["chunk_index"],
                            norm_values,
                            f"gemini-embedding-2-{TARGET_DIM}d",
                        )
                    )
                except Exception as e:
                    logger.error(f"Failed to parse embedding line {idx}: {e}")

        if tuples:
            conn = psycopg2.connect(SUPABASE_DB_URL)
            try:
                with conn:
                    with conn.cursor() as cur:
                        psycopg2.extras.execute_values(
                            cur,
                            """
                            INSERT INTO market_data.news_embeddings
                                (asset_id, news_id, chunk_index, embedding, model_ver)
                            VALUES %s
                            ON CONFLICT (asset_id, news_id, chunk_index) DO UPDATE SET
                                embedding   = EXCLUDED.embedding,
                                model_ver   = EXCLUDED.model_ver,
                                embedded_at = NOW()
                            """,
                            tuples,
                        )
            finally:
                conn.close()

    # Orchestration
    job_payload = submit_embed_batch()
    wait_payload = wait_for_embed_batch(job_payload)
    process_embed_batch(wait_payload)
