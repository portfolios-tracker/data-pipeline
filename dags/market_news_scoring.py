import json
import logging
import re
from datetime import timedelta

import psycopg2
import psycopg2.extras
from airflow import DAG
from airflow.sdk import task
from airflow.sdk.bases.sensor import PokeReturnValue

from dags.etl_modules.gemini_helpers import (
    SUPABASE_DB_URL,
    get_gemini_api_key,
    truncate_text,
)

logger = logging.getLogger(__name__)

default_args = {
    "owner": "data_engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="market_news_scoring",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["news", "scoring", "gemini-batch"],
) as dag:

    @task
    def submit_score_batch() -> str | None:
        """Submits a BatchJob to Gemini and returns the job name."""
        from google import genai

        api_key = get_gemini_api_key()
        if not api_key:
            logger.warning("GEMINI_API_KEY not found. Skipping.")
            return None

        conn = psycopg2.connect(SUPABASE_DB_URL)
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT asset_id, news_id, title, news_content as description
                    FROM market_data.news
                    WHERE sentiment_score IS NULL
                    ORDER BY publish_date DESC
                    LIMIT 500
                    """
                )
                data = cur.fetchall()
        finally:
            conn.close()

        if not data:
            logger.info("No unscored news found.")
            return None

        inline_requests = []
        for item in data:
            title = item.get("title", "No Title")
            desc = truncate_text(item.get("description", ""), 1000)
            prompt = f"Analyze sentiment for the following financial news item from the Vietnamese stock market. Return a single numeric score between -1.0 (extremely negative) and 1.0 (extremely positive). 0.0 is neutral. Just return the number, nothing else.\n\nTitle: {title}\nContent: {desc}"

            # Use news_id as custom ID in the userMetadata if possible, or just rely on order.
            # We will return the list of IDs to XCom so the downstream task can map them safely.
            inline_requests.append(
                {
                    "contents": [{"parts": [{"text": prompt}]}],
                }
            )

        mappings = [
            {"asset_id": str(item["asset_id"]), "news_id": item["news_id"]}
            for item in data
        ]

        client = genai.Client(api_key=api_key)
        batch_job = client.batches.create(
            model="gemini-2.5-flash-lite", src=inline_requests
        )
        logger.info(f"Submitted batch job {batch_job.name}")

        # We need both the job_name and the ordered news_ids for processing
        return json.dumps({"job_name": batch_job.name, "mappings": mappings})

    @task.sensor(poke_interval=120, timeout=7200, mode="reschedule")
    def wait_for_score_batch(job_payload: str | None) -> PokeReturnValue:
        """Polls the batch job state, freeing the worker slot in between pokes."""
        if not job_payload:
            return PokeReturnValue(is_done=True, xcom_value=None)

        payload = json.loads(job_payload)
        job_name = payload["job_name"]

        from google import genai

        client = genai.Client(api_key=get_gemini_api_key())
        batch_job = client.batches.get(name=job_name)

        logger.info(f"Batch job status: {batch_job.state.name}")

        if batch_job.state.name == "JOB_STATE_SUCCEEDED":
            return PokeReturnValue(is_done=True, xcom_value=json.dumps(payload))
        elif batch_job.state.name in ["JOB_STATE_FAILED", "JOB_STATE_CANCELLED"]:
            raise RuntimeError(f"Batch job failed: {batch_job.error}")

        return PokeReturnValue(is_done=False)

    @task
    def process_score_batch(job_payload: str | None) -> None:
        """Parses the inline JSONL output and updates the database."""
        if not job_payload:
            return

        payload = json.loads(job_payload)
        job_name = payload["job_name"]
        mappings = payload["mappings"]

        from google import genai

        client = genai.Client(api_key=get_gemini_api_key())
        batch_job = client.batches.get(name=job_name)

        batch_updates = []
        if batch_job.dest.inlined_responses:
            for idx, resp in enumerate(batch_job.dest.inlined_responses):
                try:
                    response_text = resp.response.candidates[0].content.parts[0].text
                    match = re.search(r"-?\d+\.\d+|-?\d+", response_text)
                    score = float(match.group()) if match else 0.0
                    sentiment_score = max(-1.0, min(1.0, score))

                    m = mappings[idx]
                    batch_updates.append((sentiment_score, m["asset_id"], m["news_id"]))
                except Exception as e:
                    logger.error(f"Failed to parse line {idx}: {e}")
                    m = mappings[idx]
                    batch_updates.append((0.0, m["asset_id"], m["news_id"]))

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
                            FROM (VALUES %s) AS v(sentiment_score, asset_id, news_id)
                            WHERE n.asset_id = v.asset_id::uuid
                              AND n.news_id = v.news_id::bigint
                            """,
                            batch_updates,
                        )
            finally:
                conn.close()
            logger.info(f"Saved sentiment scores for {len(batch_updates)} news items.")

    # Orchestration
    job_payload = submit_score_batch()
    wait_payload = wait_for_score_batch(job_payload)
    process_score_batch(wait_payload)
