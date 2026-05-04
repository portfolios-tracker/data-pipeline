"""
Upsert scraped news JSON → Supabase market_data.news table.

Supports both theinvestor.vn and bizhub.vietnamnews.vn JSON exports.
Skips articles that already exist (ON CONFLICT DO NOTHING).

Usage:
    SUPABASE_DB_URL=... python upsert_scraped_news.py <file1.json> [file2.json ...]

Example:
    SUPABASE_DB_URL=... python upsert_scraped_news.py theinvestor.json bizhub.json
"""

import argparse
import json
import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

SUPABASE_DB_URL = __import__("os").getenv("SUPABASE_DB_URL")

# ── news_id extraction ────────────────────────────────────────────────────────
_THEINVESTOR_ID_RE = re.compile(r"-d(\d+)\.html$", re.IGNORECASE)
_BIZHUB_ID_RE = re.compile(r"-post(\d+)\.html$", re.IGNORECASE)


def _extract_news_id(url: str) -> int | None:
    for pattern in (_THEINVESTOR_ID_RE, _BIZHUB_ID_RE):
        match = pattern.search(url)
        if match:
            return int(match.group(1))
    return None


def _detect_source(source_url_or_meta: str) -> str:
    """Infer source label from the top-level 'source' field or article URL."""
    if "theinvestor" in source_url_or_meta:
        return "theinvestor"
    if "bizhub" in source_url_or_meta or "vietnamnews" in source_url_or_meta:
        return "bizhub"
    return "unknown"


# ── normalise a single article dict from JSON ─────────────────────────────────
def _normalise(article: dict, source_label: str) -> dict | None:
    url = article.get("url", "").strip()
    news_id = _extract_news_id(url)
    if not news_id:
        logger.warning("Could not extract news_id from URL: %s — skipping", url)
        return None

    title = (article.get("title") or "").strip()
    body = (article.get("body") or article.get("news_content") or "").strip()

    raw_date = article.get("published_date") or article.get("publish_date")
    try:
        publish_date = (
            datetime.fromisoformat(raw_date) if raw_date else datetime.now(timezone.utc)
        )
    except (ValueError, TypeError):
        publish_date = datetime.now(timezone.utc)

    return {
        "news_id": news_id,
        "title": title,
        "news_content": body,
        "publish_date": publish_date,
        "source": source_label,
        "source_url": url,
        "source_type": "scraped",
    }


# ── load + normalise one JSON file ────────────────────────────────────────────
def load_file(path: Path) -> list[dict]:
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    # top-level "source" field identifies the feed origin
    source_label = _detect_source(data.get("source", ""))
    raw_articles = data.get("articles", [])

    logger.info(
        "Loaded %s articles from %s (source=%s)",
        len(raw_articles),
        path.name,
        source_label,
    )

    rows = []
    for article in raw_articles:
        row = _normalise(article, source_label)
        if row:
            rows.append(row)

    skipped = len(raw_articles) - len(rows)
    if skipped:
        logger.warning(
            "Skipped %s articles with unparseable URLs in %s", skipped, path.name
        )

    return rows


# ── upsert to DB ──────────────────────────────────────────────────────────────
def upsert(rows: list[dict]) -> tuple[int, int]:
    """
    Upserts rows into market_data.news.
    Returns (attempted, inserted) counts.
    ON CONFLICT (news_id, source) DO NOTHING — existing articles are left untouched.
    """
    if not rows:
        return 0, 0

    tuples = [
        (
            r["news_id"],
            r["title"],
            r["news_content"],
            r["publish_date"],
            r["source"],
            r["source_url"],
            r["source_type"],
        )
        for r in rows
    ]

    conn = psycopg2.connect(SUPABASE_DB_URL)
    try:
        with conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO market_data.news
                        (news_id, title, news_content, publish_date,
                         source, source_url, source_type)
                    VALUES %s
                    ON CONFLICT (news_id) WHERE source_type = 'scraped' DO NOTHING
                    """,
                    tuples,
                )
                inserted = cur.rowcount  # rows actually written (excludes conflicts)
    finally:
        conn.close()

    return len(tuples), inserted


# ── entrypoint ────────────────────────────────────────────────────────────────
def main() -> None:
    if not SUPABASE_DB_URL:
        logger.error("SUPABASE_DB_URL is not set.")
        sys.exit(1)

    parser = argparse.ArgumentParser(description="Upsert scraped news JSON to Supabase")
    parser.add_argument("files", nargs="+", help="Path(s) to scraped JSON files")
    args = parser.parse_args()

    all_rows: list[dict] = []
    for filepath in args.files:
        path = Path(filepath)
        if not path.exists():
            logger.error("File not found: %s", filepath)
            sys.exit(1)
        all_rows.extend(load_file(path))

    if not all_rows:
        logger.info("No valid rows to upsert.")
        return

    # Deduplicate within the loaded batch itself (same news_id + source)
    seen: set[tuple] = set()
    deduped = []
    for row in all_rows:
        key = (row["news_id"], row["source"])
        if key not in seen:
            seen.add(key)
            deduped.append(row)

    dupes_in_batch = len(all_rows) - len(deduped)
    if dupes_in_batch:
        logger.info("Dropped %s intra-batch duplicates", dupes_in_batch)

    logger.info("Upserting %s rows to market_data.news...", len(deduped))
    attempted, inserted = upsert(deduped)
    skipped = attempted - inserted

    logger.info(
        "Done — attempted=%s, inserted=%s, skipped(already existed)=%s",
        attempted,
        inserted,
        skipped,
    )


if __name__ == "__main__":
    main()
