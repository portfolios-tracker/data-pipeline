from airflow import DAG
from airflow.sdk import task
from datetime import datetime, timedelta
from pendulum import timezone
import pandas as pd
import psycopg2
import psycopg2.extras
import os
import sys
import re

# Add dags directory to path so we can import etl_modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl_modules.fetcher import fetch_news, get_active_vn_stock_tickers
from etl_modules.notifications import (
    send_success_notification,
    send_failure_notification,
    send_telegram_news_summary,
)

SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

default_args = {
    "owner": "data_engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# Set timezone to Vietnam (UTC+7)
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
    def extract_news():
        news_data = []
        # get_active_vn_stock_tickers returns list[dict] with {"symbol": "...", "asset_id": "..."}
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
            
        import logging
        import json
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
        logger.info(f"Scoring sentiment for {total_items} news items in batches of {batch_size}...")

        for i in range(0, total_items, batch_size):
            batch = data[i:i + batch_size]
            
            # Prepare batch prompt
            items_text = ""
            for idx, row in enumerate(batch):
                title = row.get("title", "No Title")
                desc = row.get("description", "")
                items_text += f"ID: {idx}\nTitle: {title}\nContent: {desc}\n---\n"
            
            prompt = f"""
            Analyze the sentiment of the following {len(batch)} financial news items for the Vietnamese stock market.
            For each item, provide a sentiment score between -1.0 (extremely negative) and 1.0 (extremely positive). 
            0.0 is neutral.
            
            Return the results as a JSON object with a key "scores" containing an array of objects, 
            each with "id" (the ID provided in the prompt) and "score" (the numeric sentiment score).
            
            Items:
            {items_text}
            """
            
            try:
                response = client.models.generate_content(
                    model="gemini-2.0-flash",
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json",
                    )
                )

                try:
                    # Robust cleaning of response.text to remove potential markdown blocks
                    raw_text = response.text.strip()
                    if raw_text.startswith("```"):
                        # Extract JSON content from within code blocks
                        match = re.search(r"\{.*\}", raw_text, re.DOTALL)
                        if match:
                            raw_text = match.group(0)

                    result = json.loads(raw_text)
                    scores_list = result.get("scores", [])
                    scores_map = {item["id"]: item["score"] for item in scores_list if "id" in item and "score" in item}

                    for idx, row in enumerate(batch):                        # Use string idx as key if JSON conversion made IDs strings
                        score = scores_map.get(idx, scores_map.get(str(idx), 0.0))
                        row["sentiment_score"] = max(-1.0, min(1.0, float(score)))
                except (json.JSONDecodeError, ValueError, KeyError) as e:
                    logger.error(f"Failed to parse Gemini response for batch starting at {i}: {e}")
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
            
        return data

    @task
    def load_news(data):
        if not data:
            print("No news data to load.")
            return

        if not SUPABASE_DB_URL:
            raise RuntimeError("SUPABASE_DB_URL environment variable is not set")

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

        try:
            with conn:
                with conn.cursor() as cur:
                    psycopg2.extras.execute_values(
                        cur,
                        """
                        INSERT INTO market_data.news
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
                        """,
                        tuples,
                    )
        finally:
            conn.close()

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

