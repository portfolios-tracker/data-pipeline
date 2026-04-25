import json
import logging
from datetime import date, timedelta

import numpy as np
import psycopg2
import psycopg2.extras
from airflow import DAG
from airflow.decorators import task
from airflow.sensors.base import PokeReturnValue
from pendulum import timezone

from dags.etl_modules.gemini_helpers import SUPABASE_DB_URL, get_gemini_api_key, truncate_text

logger = logging.getLogger(__name__)

default_args = {
    "owner": "data_engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

MODEL = "gemini-embedding-001"
DIM = 768

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
            return None

        texts = [
            f"{r['title']}. {truncate_text(r['news_content'] or '', 3000)}".strip() for r in rows
        ]

        # Serialize identification data for reconstruction in processing task
        id_mappings = [{"asset_id": str(r["asset_id"]), "news_id": int(r["news_id"])} for r in rows]

        client = genai.Client(api_key=api_key)
        batch_job = client.batches.create_embeddings(
            model=MODEL,
            src=texts
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

        if batch_job.state.name == 'JOB_STATE_SUCCEEDED':
            return PokeReturnValue(is_done=True, xcom_value=json.dumps(payload))
        elif batch_job.state.name in ['JOB_STATE_FAILED', 'JOB_STATE_CANCELLED']:
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
                    values = resp.response.embeddings[0].values
                    if not values:
                        continue
                    norm_values = _normalize(values)

                    m = mappings[idx]
                    tuples.append((
                        m["asset_id"],
                        m["news_id"],
                        norm_values,
                        "gemini-embedding-001-768d",
                    ))
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

    # Orchestration
    job_payload = submit_embed_batch()
    wait_payload = wait_for_embed_batch(job_payload)
    process_embed_batch(wait_payload)
