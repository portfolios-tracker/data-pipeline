import logging
from datetime import timedelta

from airflow import DAG
from airflow.sdk import task

from dags.etl_modules.gemini_helpers import (
    SUPABASE_DB_URL,
    chunk_text,
    get_gemini_api_key,
)

logger = logging.getLogger(__name__)

default_args = {
    "owner": "data_engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

MODEL = "models/gemini-embedding-001"
TARGET_DIM = 768
EMBED_BATCH_SIZE = 100


def _normalize(vec: list[float]) -> list[float]:
    import numpy as np

    arr = np.array(vec, dtype=np.float32)
    norm = np.linalg.norm(arr)
    return (arr / norm if norm > 0 else arr).tolist()


def _iter_batches(items: list, batch_size: int) -> list[list]:
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    return [items[i : i + batch_size] for i in range(0, len(items), batch_size)]


with DAG(
    dag_id="market_news_embedding",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["news", "embedding", "gemini-direct"],
) as dag:

    @task
    def generate_and_upsert_embeddings() -> int:
        import psycopg2
        import psycopg2.extras
        from google import genai

        api_key = get_gemini_api_key()
        if not api_key:
            raise RuntimeError("GEMINI_API_KEY not found.")

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
                    LIMIT 200
                    """,
                )
                rows = cur.fetchall()
        finally:
            conn.close()

        if not rows:
            return 0

        texts_to_embed: list[str] = []
        id_mappings: list[dict] = []

        for row in rows:
            full_text = f"{row['title']}. {row['news_content'] or ''}".strip()
            chunks = chunk_text(full_text, chunk_size=4000, chunk_overlap=800)

            for chunk_index, chunk in enumerate(chunks):
                texts_to_embed.append(chunk)
                id_mappings.append(
                    {
                        "news_row_id": str(row["news_row_id"]),
                        "chunk_index": chunk_index,
                    }
                )

        client = genai.Client(api_key=api_key)
        tuples = []
        processed = 0

        for batch_texts in _iter_batches(texts_to_embed, EMBED_BATCH_SIZE):
            response = client.models.embed_content(
                model=MODEL,
                contents=batch_texts,
                config={
                    "output_dimensionality": TARGET_DIM,
                    "task_type": "RETRIEVAL_DOCUMENT",
                },
            )

            if len(response.embeddings) != len(batch_texts):
                raise ValueError(
                    "Embedding count mismatch: "
                    f"{len(response.embeddings)} embeddings for "
                    f"{len(batch_texts)} chunks"
                )

            for offset, embedding in enumerate(response.embeddings):
                mapping = id_mappings[processed + offset]
                values = embedding.values
                if not values:
                    logger.warning(
                        "Empty embedding: %s[%s]",
                        mapping["news_row_id"],
                        mapping["chunk_index"],
                    )
                    continue

                tuples.append(
                    (
                        mapping["news_row_id"],
                        mapping["chunk_index"],
                        _normalize(values),
                        f"gemini-001-{TARGET_DIM}d",
                    )
                )

            processed += len(batch_texts)

        if tuples:
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
                                embedding = EXCLUDED.embedding,
                                model_ver = EXCLUDED.model_ver,
                                embedded_at = NOW()
                            """,
                            tuples,
                        )
            finally:
                conn.close()

        return len(tuples)

    generate_and_upsert_embeddings()
