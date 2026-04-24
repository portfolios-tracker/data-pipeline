<<<<<<< HEAD
import os
import re
=======
import logging
import os
import pickle
>>>>>>> ae3cabc (feat: Market News DAG Refactor — Multi-Source + FinBERT + Embeddings)
from datetime import datetime, timedelta

import pandas as pd
import psycopg2
import psycopg2.extras
from airflow import DAG
from airflow.sdk import task
from pendulum import timezone

<<<<<<< HEAD
from dags.etl_modules.fetcher import fetch_news, get_active_vn_stock_tickers
from dags.etl_modules.notifications import (
    send_failure_notification,
    send_success_notification,
    send_telegram_news_summary,
)

SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
=======
from dags.etl_modules.extractors import run_all_extractors
from dags.etl_modules.notifications import (
    send_failure_notification,
    send_success_notification,
)

logger = logging.getLogger(__name__)

SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")
>>>>>>> ae3cabc (feat: Market News DAG Refactor — Multi-Source + FinBERT + Embeddings)

default_args = {
    "owner": "data_engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

<<<<<<< HEAD
# Set timezone to Vietnam (UTC+7)
local_tz = timezone("Asia/Bangkok")

with DAG(
    dag_id="market_news_morning",
    default_args=default_args,
    schedule="0 7 * * 1-5",  # 7 AM Vietnam Time Mon-Fri
=======
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
            "Local model cache missing — downloading %s from Supabase Storage", model_ver
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
>>>>>>> ae3cabc (feat: Market News DAG Refactor — Multi-Source + FinBERT + Embeddings)
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),
    catchup=False,
    tags=["news", "supabase", "morning-brief"],
    on_success_callback=send_success_notification,
    on_failure_callback=send_failure_notification,
) as dag:

    @task
<<<<<<< HEAD
    def extract_news():
        news_data = []
        # get_active_vn_stock_tickers returns list[dict] with
        # {"symbol": "...", "asset_id": "..."}
        tickers_info = get_active_vn_stock_tickers(raise_on_fallback=True)
        print(f"Fetching daily news for {len(tickers_info)} tickers...")

        for info in tickers_info:
            symbol = info["symbol"]
            asset_id = info["asset_id"]
            df = fetch_news(symbol, asset_id)
            if not df.empty:
                news_data.append(df)

        if news_data:
            final_df = pd.concat(news_data)
            # Convert Timestamp objects to strings for JSON serialization
            if "publish_date" in final_df.columns:
                final_df["publish_date"] = final_df["publish_date"].astype(str)
            return final_df.to_dict("records")
        return []

    @task
    def score_news(data):
        """Add sentiment score to each news record using Gemini with batching"""
        if not data:
            return []

        import json
        import logging

        from google import genai
        from google.genai import errors, types

        logger = logging.getLogger("airflow.task")

        if not GOOGLE_API_KEY:
            logger.warning("GOOGLE_API_KEY not set. Skipping sentiment scoring.")
            for row in data:
                row["sentiment_score"] = 0.0
            return data

        try:
            client = genai.Client(api_key=GOOGLE_API_KEY)
        except Exception as e:
            logger.error(f"Failed to initialize Gemini client: {e}")
            raise

        batch_size = 15
        total_items = len(data)
        logger.info(
            "Scoring sentiment for "
            f"{total_items} news items in batches of {batch_size}..."
        )

        for i in range(0, total_items, batch_size):
            batch = data[i : i + batch_size]

            # Prepare batch prompt
            items_text = ""
            for idx, row in enumerate(batch):
                title = row.get("title", "No Title")
                desc = row.get("description", "")
                items_text += f"ID: {idx}\nTitle: {title}\nContent: {desc}\n---\n"

            prompt = f"""
            Analyze sentiment for the following {len(batch)} financial news items
            from the Vietnamese stock market.
            For each item, provide a sentiment score between -1.0
            (extremely negative) and 1.0 (extremely positive).
            0.0 is neutral.

            Return results as JSON with key "scores" containing an array.
            Each item must include "id" (the ID in this prompt)
            and "score" (numeric sentiment score).

            Items:
            {items_text}
            """

            try:
                response = client.models.generate_content(
                    model="gemini-2.0-flash",
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json",
                    ),
                )

                try:
                    # Clean response.text to remove potential markdown blocks.
                    raw_text = (response.text or "").strip()
                    if raw_text.startswith("```"):
                        # Extract JSON content from within code blocks
                        match = re.search(r"\{.*\}", raw_text, re.DOTALL)
                        if match:
                            raw_text = match.group(0)

                    result = json.loads(raw_text)
                    scores_list = result.get("scores", [])
                    scores_map = {
                        item["id"]: item["score"]
                        for item in scores_list
                        if "id" in item and "score" in item
                    }

                    for idx, row in enumerate(
                        batch
                    ):  # Use string idx as key if JSON conversion made IDs strings
                        score = scores_map.get(idx, scores_map.get(str(idx), 0.0))
                        row["sentiment_score"] = max(-1.0, min(1.0, float(score)))
                except (json.JSONDecodeError, ValueError, KeyError) as e:
                    logger.error(
                        "Failed to parse Gemini response for batch "
                        f"starting at {i}: {e}"
                    )
                    logger.debug(f"Raw response: {response.text}")
                    for row in batch:
                        row["sentiment_score"] = 0.0

            except errors.APIError as e:
                logger.error(f"Gemini API Error (batch starting at {i}): {e}")
                # Re-raise to let Airflow handle retries or alerting
                raise
            except Exception as e:
                logger.error(f"Unexpected error scoring batch starting at {i}: {e}")
                raise
=======
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
>>>>>>> ae3cabc (feat: Market News DAG Refactor — Multi-Source + FinBERT + Embeddings)

        return data

    @task
<<<<<<< HEAD
    def load_news(data):
        if not data:
            print("No news data to load.")
=======
    def load_news(data: list[dict]) -> None:
        if not data:
            logger.info("No news to load.")
>>>>>>> ae3cabc (feat: Market News DAG Refactor — Multi-Source + FinBERT + Embeddings)
            return

        if not SUPABASE_DB_URL:
            raise RuntimeError("SUPABASE_DB_URL environment variable is not set")

<<<<<<< HEAD
        print("Connecting to Supabase/Postgres...")
        conn = psycopg2.connect(SUPABASE_DB_URL)

        print(f"Inserting {len(data)} news rows into market_data.news...")
        cols = [
            "asset_id",
            "publish_date",
            "title",
            "source",
            "price_at_publish",
            "price_change",
            "price_change_ratio",
            "sentiment_score",
            "news_id",
        ]
        tuples = []
        for row in data:
            # Convert publish_date back to datetime object
            if row.get("publish_date"):
                try:
                    row["publish_date"] = pd.to_datetime(row["publish_date"])
                except Exception:
                    pass
            tuples.append([row.get(c) for c in cols])

=======
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
>>>>>>> ae3cabc (feat: Market News DAG Refactor — Multi-Source + FinBERT + Embeddings)
        try:
            with conn:
                with conn.cursor() as cur:
                    psycopg2.extras.execute_values(
                        cur,
                        """
                        INSERT INTO market_data.news
<<<<<<< HEAD
                            (asset_id, publish_date, title, source, price_at_publish,
                             price_change, price_change_ratio, sentiment_score, news_id)
                        VALUES %s
                        ON CONFLICT (asset_id, news_id) DO UPDATE SET
                            publish_date = EXCLUDED.publish_date,
                            title = EXCLUDED.title,
                            source = EXCLUDED.source,
                            price_at_publish = EXCLUDED.price_at_publish,
                            price_change = EXCLUDED.price_change,
                            price_change_ratio = EXCLUDED.price_change_ratio,
                            sentiment_score = EXCLUDED.sentiment_score,
                            ingested_at = NOW()
=======
                            (asset_id, news_id, publish_date, title,
                             news_content, source, source_url, sentiment_score)
                        VALUES %s
                        ON CONFLICT (asset_id, news_id) DO UPDATE SET
                            sentiment_score = EXCLUDED.sentiment_score,
                            ingested_at     = NOW()
>>>>>>> ae3cabc (feat: Market News DAG Refactor — Multi-Source + FinBERT + Embeddings)
                        """,
                        tuples,
                    )
        finally:
            conn.close()

<<<<<<< HEAD
        print("News insertion complete.")

    @task
    def send_news_digest(data):
        """Send news summary to Telegram"""
        if data:
            send_telegram_news_summary(data)
            print(f"Sent {len(data)} news items to Telegram")
        else:
            print("No news to send")

    # Orchestration
    raw_news = extract_news()
    scored_news = score_news(raw_news)
    load_news(scored_news)
    send_news_digest(scored_news)
=======
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

            texts = [
                f"{r['title']} {r['news_content'] or ''}".strip()
                for r in rows
            ]

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
>>>>>>> ae3cabc (feat: Market News DAG Refactor — Multi-Source + FinBERT + Embeddings)
