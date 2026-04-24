import logging
import os
import pickle
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

default_args = {
    "owner": "data_engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

local_tz = timezone("Asia/Bangkok")


def _build_finbert_input(row: dict) -> str:
    title = row.get("title", "")
    content = row.get("news_content", "")
    lead = ". ".join(content.split(".")[:3]) if content else ""
    return f"{title}. {lead}".strip()


def _download_models_from_supabase(model_ver: str, local_dir: str) -> None:
    """Download tfidf.pkl and svd.pkl from Supabase Storage to local_dir."""
    from supabase import create_client

    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not supabase_url or not supabase_key:
        raise RuntimeError(
            "SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set "
            "to download models from Supabase Storage"
        )

    client = create_client(supabase_url, supabase_key)
    os.makedirs(local_dir, exist_ok=True)

    for filename in ("tfidf.pkl", "svd.pkl"):
        storage_path = f"models/{model_ver}/{filename}"
        data = client.storage.from_("market-data").download(storage_path)
        local_path = os.path.join(local_dir, filename)
        with open(local_path, "wb") as fh:
            fh.write(data)
        logger.info("Downloaded %s → %s", storage_path, local_path)


def _load_canonical_models(model_ver: str = "tfidf-svd-v1"):
    local_dir = f"/opt/airflow/models/{model_ver}"
    tfidf_path = os.path.join(local_dir, "tfidf.pkl")
    svd_path = os.path.join(local_dir, "svd.pkl")

    if not (os.path.exists(tfidf_path) and os.path.exists(svd_path)):
        logger.info(
            "Local model cache missing — downloading %s from Supabase Storage",
            model_ver,
        )
        _download_models_from_supabase(model_ver, local_dir)

    with open(tfidf_path, "rb") as fh:
        tfidf = pickle.load(fh)
    with open(svd_path, "rb") as fh:
        svd = pickle.load(fh)

    logger.info("Canonical model %s loaded from %s", model_ver, local_dir)
    return tfidf, svd


with DAG(
    dag_id="market_news_morning",
    default_args=default_args,
    schedule="0 7 * * 1-5",
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),
    catchup=False,
    tags=["news", "supabase", "morning-brief"],
    on_success_callback=send_success_notification,
    on_failure_callback=send_failure_notification,
) as dag:

    @task
    def extract_news() -> list[dict]:
        data = run_all_extractors()
        for row in data:
            if row.get("publish_date") and not isinstance(row["publish_date"], str):
                row["publish_date"] = str(row["publish_date"])
        logger.info("extract_news: %s records from all sources", len(data))
        return data

    @task
    def score_sentiment(data: list[dict]) -> list[dict]:
        if not data:
            return []

        import torch
        from transformers import (
            BertForSequenceClassification,
            BertTokenizer,
            pipeline,
        )

        device = 0 if torch.cuda.is_available() else -1
        BATCH = 32

        logger.info(
            "FinBERT scoring %s articles (batch=%s, device=%s)",
            len(data),
            BATCH,
            "GPU" if device == 0 else "CPU",
        )

        tokenizer = BertTokenizer.from_pretrained("ProsusAI/finbert")
        model = BertForSequenceClassification.from_pretrained("ProsusAI/finbert")
        nlp = pipeline(
            "sentiment-analysis",
            model=model,
            tokenizer=tokenizer,
            device=device,
            truncation=True,
            max_length=512,
            top_k=None,
        )

        texts = [_build_finbert_input(r) for r in data]

        all_results = []
        for i in range(0, len(texts), BATCH):
            all_results.extend(nlp(texts[i : i + BATCH]))

        for row, label_scores in zip(data, all_results):
            try:
                scores = {item["label"].lower(): item["score"] for item in label_scores}
                row["sentiment_score"] = round(
                    scores.get("positive", 0.0) - scores.get("negative", 0.0), 4
                )
            except Exception as exc:
                logger.warning(
                    "Score parsing failed for news_id=%s: %s", row.get("news_id"), exc
                )
                row["sentiment_score"] = 0.0

        return data

    @task
    def load_news(data: list[dict]) -> None:
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
            "sentiment_score",
        ]
        tuples = []
        for row in data:
            if row.get("publish_date"):
                row["publish_date"] = pd.to_datetime(row["publish_date"])
            tuples.append([row.get(c) for c in cols])

        logger.info("load_news: inserting %s rows into market_data.news", len(tuples))
        conn = psycopg2.connect(SUPABASE_DB_URL)
        try:
            with conn:
                with conn.cursor() as cur:
                    psycopg2.extras.execute_values(
                        cur,
                        """
                        INSERT INTO market_data.news
                            (asset_id, news_id, publish_date, title,
                             news_content, source, source_url, sentiment_score)
                        VALUES %s
                        ON CONFLICT (asset_id, news_id) DO UPDATE SET
                            sentiment_score = EXCLUDED.sentiment_score,
                            ingested_at     = NOW()
                        """,
                        tuples,
                    )
        finally:
            conn.close()

        logger.info("load_news: complete")

    @task
    def embed_news() -> None:
        """
        Query today's unembedded articles from the DB and compute TF-IDF + SVD
        embeddings.  Reads from the DB directly — does NOT use XCom — so it is
        safe regardless of article volume.
        """
        import numpy as np
        from datetime import date

        tfidf, svd = _load_canonical_models()

        conn = psycopg2.connect(SUPABASE_DB_URL)
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT n.asset_id, n.news_id, n.title, n.news_content
                    FROM market_data.news n
                    LEFT JOIN market_data.news_embeddings e
                        ON n.asset_id = e.asset_id AND n.news_id = e.news_id
                    WHERE n.publish_date::date = %s
                      AND e.news_id IS NULL
                    """,
                    (date.today(),),
                )
                rows = cur.fetchall()

            if not rows:
                logger.info("embed_news: no new articles to embed today")
                return

            texts = [f"{r['title']} {r['news_content'] or ''}".strip() for r in rows]

            matrix = tfidf.transform(texts)
            embeddings = svd.transform(matrix)

            norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
            embeddings = embeddings / np.where(norms == 0, 1, norms)

            tuples = [
                (
                    str(r["asset_id"]),
                    int(r["news_id"]),
                    embeddings[i].tolist(),
                    "tfidf-svd-v1",
                )
                for i, r in enumerate(rows)
            ]

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
            logger.info(
                "embed_news: embedded %s articles with model tfidf-svd-v1", len(tuples)
            )
        finally:
            conn.close()

    raw_news = extract_news()
    scored_news = score_sentiment(raw_news)
    load_news(scored_news)
    embed_news()
