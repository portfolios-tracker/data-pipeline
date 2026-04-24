"""TheInvestor Companies scraper.

Depth 1 (listing + pagination):
  - Page 1  -> parse listing HTML from https://theinvestor.vn/companies/
  - Page 2+ -> GET /?mod=iframe&act=load_more_home&id_last_news=<id>&cate_id=3

Depth 2 (article body):
  - webclaw CLI -> parse metadata + clean plain text body

Output: theinvestor_companies_data.json

Requirements:
    pip install requests beautifulsoup4
    webclaw CLI in PATH

Usage:
    python scripts/scrape/theinvestor_companies_extractor.py
    python scripts/scrape/theinvestor_companies_extractor.py --max-articles 50
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import requests
from bs4 import BeautifulSoup

NEWS_INDEX = "https://theinvestor.vn/companies/"
LOAD_MORE_URL = "https://theinvestor.vn/"
DEFAULT_OUTPUT_FILE = "theinvestor_companies_data.json"
DEFAULT_MAX_ARTICLES = 200
DEFAULT_MAX_WORKERS = 5
REQUEST_DELAY_SECONDS = 1.2
CATE_ID = 3

ARTICLE_URL_RE = re.compile(r"^https://theinvestor\.vn/.+-d(?P<article_id>\d+)\.html$")
STOP_MARKER_RE = re.compile(
    r"^(Tags:?|Comments\s*\(|Latest\)\s*\|\s*Most liked\)|Share:?|Related news:?|See also:?|More in)",
    re.IGNORECASE,
)
NOISE_LINE_RE = re.compile(
    r"^(?:"
    r"-\s*Companies"
    r"|-\s*Executive Talk"
    r"|-\s*Bamboo Capital"
    r"|-\s*Consulting"
    r"|By"
    r"|[A-Z][a-z]{2},\s+[A-Z][a-z]+\s+\d{1,2},\s+\d{4}\s*\|"
    r"|Photo courtesy"
    r"|\*\*"
    r")$",
    re.IGNORECASE,
)

REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": NEWS_INDEX,
}

LOAD_MORE_HEADERS = {
    "User-Agent": REQUEST_HEADERS["User-Agent"],
    "Accept": "*/*",
    "X-Requested-With": "XMLHttpRequest",
    "Referer": NEWS_INDEX,
}


@dataclass
class Config:
    output_file: str
    max_articles: int
    max_workers: int


def webclaw_scrape(url: str) -> dict[str, Any] | None:
    """Call: webclaw <url> -f json -> parsed dict or None on failure."""
    try:
        proc = subprocess.run(
            ["webclaw", url, "-f", "json"],
            capture_output=True,
            text=True,
            timeout=40,
        )
    except subprocess.TimeoutExpired:
        print(f"  [WARN] webclaw timeout: {url[:80]}")
        return None
    except FileNotFoundError:
        print("[ERROR] webclaw not found in PATH.")
        sys.exit(1)

    if proc.returncode != 0:
        print(f"  [WARN] webclaw error [{url[:80]}]: {proc.stderr.strip()[:200]}")
        return None

    try:
        return json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        print(f"  [WARN] webclaw JSON parse error [{url[:80]}]: {exc}")
        return None


def extract_article_id(url: str) -> int | None:
    match = ARTICLE_URL_RE.match(url.strip())
    if not match:
        return None
    return int(match.group("article_id"))


def parse_listing_items(html_fragment: str) -> list[dict[str, Any]]:
    """Parse row-three listing cards from HTML and return article stubs."""
    soup = BeautifulSoup(html_fragment, "html.parser")
    stubs: list[dict[str, Any]] = []

    for item in soup.select(".row-three__left-item.py-20"):
        id_attr = item.get("id-news")
        try:
            news_id = int(id_attr) if id_attr else None
        except ValueError:
            news_id = None

        title_anchor = item.select_one("h3 a[href]")
        if not title_anchor:
            title_anchor = item.select_one("a[href]")
        if not title_anchor:
            continue

        href = title_anchor.get("href", "").strip()
        if href.startswith("/"):
            href = "https://theinvestor.vn" + href

        if not ARTICLE_URL_RE.match(href):
            continue

        stubs.append(
            {
                "title": title_anchor.get_text(" ", strip=True),
                "url": href,
                "teaser": None,
                "news_id": news_id,
            }
        )

    return stubs


def fetch_index_html(session: requests.Session) -> str:
    response = session.get(NEWS_INDEX, headers=REQUEST_HEADERS, timeout=20)
    response.raise_for_status()
    return response.text


def fetch_load_more_html(session: requests.Session, id_last_news: int) -> str:
    params = {
        "mod": "iframe",
        "act": "load_more_home",
        "id_last_news": id_last_news,
        "cate_id": CATE_ID,
    }
    response = session.get(
        LOAD_MORE_URL,
        headers=LOAD_MORE_HEADERS,
        params=params,
        timeout=20,
    )
    response.raise_for_status()
    return response.text


def dedupe_stubs(stubs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    deduped: list[dict[str, Any]] = []
    for stub in stubs:
        url = stub["url"]
        if url in seen:
            continue
        seen.add(url)
        deduped.append(stub)
    return deduped


def scrape_all_pages(config: Config) -> list[dict[str, Any]]:
    """Collect Companies article stubs from page 1 and load-more pages."""
    print("[Depth 1] Collecting article URLs from TheInvestor Companies...")

    session = requests.Session()
    index_html = fetch_index_html(session)
    all_stubs = parse_listing_items(index_html)
    all_stubs = dedupe_stubs(all_stubs)

    print(f"  [Page 1] listing cards: {len(all_stubs)}")
    if not all_stubs:
        return []

    # The site uses the last card id as id_last_news for the next batch.
    last_news_id = all_stubs[-1].get("news_id")
    if not isinstance(last_news_id, int):
        fallback_ids = [extract_article_id(s["url"]) for s in all_stubs]
        fallback_ids = [x for x in fallback_ids if x is not None]
        last_news_id = min(fallback_ids) if fallback_ids else None

    if last_news_id is None:
        print("  [WARN] Could not derive pagination cursor from first page.")
        return all_stubs[: config.max_articles]

    seen_urls = {stub["url"] for stub in all_stubs}

    page = 2
    while len(all_stubs) < config.max_articles:
        time.sleep(REQUEST_DELAY_SECONDS)
        try:
            html_fragment = fetch_load_more_html(session, last_news_id)
        except requests.RequestException as exc:
            print(f"  [WARN] load-more request failed at cursor {last_news_id}: {exc}")
            break

        if not html_fragment.strip():
            print("  [Info] load-more returned empty response; stopping.")
            break

        page_stubs = parse_listing_items(html_fragment)
        if not page_stubs:
            print("  [Info] no listing cards in load-more response; stopping.")
            break

        new_count = 0
        for stub in page_stubs:
            if stub["url"] in seen_urls:
                continue
            seen_urls.add(stub["url"])
            all_stubs.append(stub)
            new_count += 1

        print(
            f"  [Page {page}] load-more cursor={last_news_id}: "
            f"cards={len(page_stubs)} new={new_count} total={len(all_stubs)}"
        )

        next_cursor = page_stubs[-1].get("news_id")
        if not isinstance(next_cursor, int) or next_cursor >= last_news_id:
            print("  [Info] invalid or non-decreasing next cursor; stopping.")
            break

        if new_count == 0:
            print("  [Info] no new URLs found in this batch; stopping.")
            break

        last_news_id = next_cursor
        page += 1

    return all_stubs[: config.max_articles]


def clean_body(plain_text: str, title: str) -> str:
    lines = [line.strip() for line in plain_text.splitlines()]

    body_lines: list[str] = []
    started = False
    for line in lines:
        if not line:
            if started:
                body_lines.append("")
            continue

        if line == title:
            continue

        if STOP_MARKER_RE.search(line):
            break

        if NOISE_LINE_RE.search(line):
            continue

        if not started:
            # Start when we hit a normal content sentence.
            if len(line) < 20:
                continue
            started = True

        body_lines.append(line)

    cleaned: list[str] = []
    prev: str | None = None
    for line in body_lines:
        if line == prev and line != "":
            continue
        cleaned.append(line)
        prev = line

    text = "\n".join(cleaned)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def extract_metadata(raw: dict[str, Any]) -> dict[str, Any]:
    metadata = raw.get("metadata", {})
    structured_data = raw.get("structured_data", [])

    news_article = {}
    for item in structured_data:
        if isinstance(item, dict) and item.get("@type") == "NewsArticle":
            news_article = item
            break

    author = metadata.get("author", "")
    sd_author = news_article.get("author")
    if isinstance(sd_author, dict):
        author = sd_author.get("name") or author

    links = raw.get("content", {}).get("links", [])
    tags = [
        link.get("text", "").strip()
        for link in links
        if isinstance(link, dict)
        and re.search(r"-tag\d+/?$", (link.get("href") or ""))
        and link.get("text")
    ]

    return {
        "title": news_article.get("headline") or metadata.get("title", ""),
        "description": news_article.get("description")
        or metadata.get("description", ""),
        "published_date": news_article.get("datePublished")
        or metadata.get("published_date", ""),
        "author": author,
        "word_count": metadata.get("word_count"),
        "tags": sorted(set(tags)),
    }


def fetch_and_clean(stub: dict[str, Any]) -> dict[str, Any]:
    raw = webclaw_scrape(stub["url"])
    if not raw:
        return {
            "title": stub["title"],
            "url": stub["url"],
            "published_date": None,
            "description": None,
            "author": None,
            "tags": [],
            "word_count": None,
            "body": None,
        }

    metadata = extract_metadata(raw)
    plain_text = raw.get("content", {}).get("plain_text", "")

    title = metadata["title"] or stub["title"]
    return {
        "title": title,
        "url": stub["url"],
        "published_date": metadata["published_date"],
        "description": metadata["description"],
        "author": metadata["author"],
        "tags": metadata["tags"],
        "word_count": metadata["word_count"],
        "body": clean_body(plain_text, title=title),
    }


def fetch_all_articles(
    stubs: list[dict[str, Any]], max_workers: int
) -> list[dict[str, Any]]:
    print(
        f"\n[Depth 2] Fetching and cleaning {len(stubs)} articles (workers={max_workers})"
    )

    results: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(fetch_and_clean, stub): stub for stub in stubs}
        for idx, future in enumerate(as_completed(futures), start=1):
            stub = futures[future]
            try:
                article = future.result()
            except Exception as exc:
                print(f"  [{idx:>3}/{len(stubs)}] FAIL {stub['title'][:70]} ({exc})")
                article = {
                    "title": stub["title"],
                    "url": stub["url"],
                    "published_date": None,
                    "description": None,
                    "author": None,
                    "tags": [],
                    "word_count": None,
                    "body": None,
                }

            words = len((article.get("body") or "").split())
            status = "OK" if article.get("body") else "MISS"
            print(
                f"  [{idx:>3}/{len(stubs)}] {status:<4} {words:>4}w  {article['title'][:75]}"
            )
            results.append(article)

    success_count = sum(1 for article in results if article.get("body"))
    print(f"\n  Completed: {success_count}/{len(results)} articles with body content")
    return results


def save(articles: list[dict[str, Any]], output_file: str) -> None:
    payload = {
        "scraped_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "source": NEWS_INDEX,
        "count": len(articles),
        "articles": articles,
    }
    with open(output_file, "w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)

    print(f"\nSaved: {output_file} ({len(articles)} articles)")


def preview(articles: list[dict[str, Any]]) -> None:
    print("\nPreview: first 2 cleaned articles")
    for article in articles[:2]:
        print(f"\nTitle : {article['title']}")
        print(f"Date  : {article['published_date']}")
        print(f"Author: {article['author']}")
        print(f"Tags  : {', '.join(article['tags']) if article['tags'] else '-'}")
        body = article.get("body") or ""
        excerpt = body[:260].replace("\n", " ")
        print(f"Body  : {excerpt}{'...' if len(body) > 260 else ''}")


def parse_args() -> Config:
    parser = argparse.ArgumentParser(description="Scrape TheInvestor Companies news")
    parser.add_argument(
        "--max-articles",
        type=int,
        default=DEFAULT_MAX_ARTICLES,
        help=f"Maximum articles to scrape (default: {DEFAULT_MAX_ARTICLES})",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=DEFAULT_MAX_WORKERS,
        help=f"Parallel article workers (default: {DEFAULT_MAX_WORKERS})",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=DEFAULT_OUTPUT_FILE,
        help=f"Output JSON file (default: {DEFAULT_OUTPUT_FILE})",
    )

    args = parser.parse_args()

    if args.max_articles <= 0:
        raise ValueError("--max-articles must be > 0")
    if args.max_workers <= 0:
        raise ValueError("--max-workers must be > 0")

    return Config(
        output_file=args.output,
        max_articles=args.max_articles,
        max_workers=args.max_workers,
    )


def main() -> None:
    try:
        config = parse_args()
    except ValueError as exc:
        print(f"[ERROR] {exc}")
        sys.exit(2)

    print("=" * 70)
    print("TheInvestor Companies Extractor")
    print("=" * 70)

    stubs = scrape_all_pages(config)
    if not stubs:
        print("No article URLs found.")
        return

    print(f"\nTotal stubs collected: {len(stubs)}")
    articles = fetch_all_articles(stubs, max_workers=config.max_workers)
    save(articles, config.output_file)
    preview(articles)


if __name__ == "__main__":
    main()
