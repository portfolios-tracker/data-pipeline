"""
news_extractor.py
─────────────────
Big Data course project — Paginated 2-depth news scraper.

Depth 1 (index + pagination):
  - Page 1  → webclaw CLI  (SSR HTML, already rendered)
  - Page 2+ → ASP.NET ASMX API  (LoadMoreArticle endpoint)

Depth 2 (article body):
  - webclaw CLI → clean body extraction

Output: news_data.json

Requirements:
    pip install requests beautifulsoup4
    webclaw CLI in PATH (see install.sh)

Usage:
    python news_extractor.py
"""

import re
import json
import subprocess
import sys
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup
import ssl
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager

# ── Configuration ─────────────────────────────────────────────────────────────

NEWS_INDEX = "https://bizhub.vietnamnews.vn/news"
OUTPUT_FILE = "news_data.json"
MAX_ARTICLES = 200  # None = scrape all pages; set e.g. 50 to cap
MAX_WORKERS = 5  # parallel article fetches
REQUEST_DELAY = 2  # seconds between paginated API calls (be polite)

# ── ASMX Pagination Config ────────────────────────────────────────────────────
ASMX_URL = "https://bizhub.vietnamnews.vn/ajaxloads/servicedata.asmx/LoadMoreArticle"
ASMX_SITEID = 1
ASMX_CATECODE = "news"
ASMX_ISPARRENT = 0
ASMX_PAGESIZE = 20

ASMX_HEADERS = {
    "Content-Type": "application/json; charset=UTF-8",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": NEWS_INDEX,
}


# ── webclaw CLI wrapper ───────────────────────────────────────────────────────


def webclaw_scrape(url: str) -> dict | None:
    """Call: webclaw <url> -f json → parsed dict or None on failure."""
    try:
        proc = subprocess.run(
            ["webclaw", url, "-f", "json"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if proc.returncode != 0:
            print(f"  ⚠  CLI error [{url[:60]}]: {proc.stderr.strip()[:100]}")
            return None
        return json.loads(proc.stdout)
    except subprocess.TimeoutExpired:
        print(f"  ⚠  Timeout: {url[:60]}")
        return None
    except json.JSONDecodeError as e:
        print(f"  ⚠  JSON parse error [{url[:60]}]: {e}")
        return None
    except FileNotFoundError:
        print("❌  'webclaw' not found in PATH. Run install.sh first.")
        sys.exit(1)


# ── Depth 1a: Page 1 via webclaw (SSR) ───────────────────────────────────────


def scrape_page1() -> list[dict]:
    """Scrape the first page of articles using webclaw (SSR content)."""
    print(f"  [Page  1] webclaw CLI → {NEWS_INDEX}")
    raw = webclaw_scrape(NEWS_INDEX)
    if not raw:
        return []

    markdown = raw.get("content", {}).get("markdown", "")
    pattern = re.compile(
        r"###\s+\[(?P<title>[^\]]+)\]\((?P<url>https://bizhub\.vietnamnews\.vn/[^\)]+)\)"
        r"(?:.*?\*\*(?P<date>[A-Za-z]+,\s+[A-Za-z]+\s+\d+,\s+\d+))?"
        r"(?:\n+(?P<teaser>[^\n#\-\*\[]{20,}))?",
        re.DOTALL,
    )

    stubs = []
    seen = set()
    for m in pattern.finditer(markdown):
        url = m.group("url").strip()
        if url in seen:
            continue
        seen.add(url)
        stubs.append(
            {
                "title": m.group("title").strip(),
                "url": url,
                "teaser": (m.group("teaser") or "").strip() or None,
            }
        )

    print(f"           {len(stubs)} articles found.")
    return stubs


# ── Depth 1b: Pages 2+ via ASMX API ──────────────────────────────────────────


def parse_stubs_from_html(html_fragment: str) -> list[dict]:
    """
    Extract article stubs from the HTML chunk returned by LoadMoreArticle.
    The site renders <h3 class="meta-data-tit"><a href="...">Title</a></h3>
    """
    soup = BeautifulSoup(html_fragment, "html.parser")
    stubs = []

    # Primary selector — confirmed by Gemini's analysis
    for h3 in soup.find_all("h3", class_="meta-data-tit"):
        a = h3.find("a", href=True)
        if not a:
            continue
        href = a["href"].strip()
        # Make absolute if relative
        if href.startswith("/"):
            href = "https://bizhub.vietnamnews.vn" + href
        stubs.append({"title": a.get_text(strip=True), "url": href, "teaser": None})

    # Fallback: any <h3> with an <a> pointing to an article URL
    if not stubs:
        for h3 in soup.find_all("h3"):
            a = h3.find("a", href=re.compile(r"bizhub\.vietnamnews\.vn/.+-post\d+"))
            if a:
                stubs.append(
                    {
                        "title": a.get_text(strip=True),
                        "url": a["href"].strip(),
                        "teaser": None,
                    }
                )

    return stubs


class SSLContextAdapter(HTTPAdapter):
    """Transport adapter that allows using a custom SSLContext with urllib3.

    This is used to enable legacy renegotiation flags when connecting to
    servers that require unsafe legacy renegotiation.
    """

    def __init__(self, ssl_context=None, **kwargs):
        self.ssl_context = ssl_context
        super().__init__(**kwargs)

    def init_poolmanager(self, connections, maxsize, block=False, **pool_kwargs):
        self.poolmanager = PoolManager(
            num_pools=connections,
            maxsize=maxsize,
            block=block,
            ssl_context=self.ssl_context,
            **pool_kwargs,
        )

    def proxy_manager_for(self, proxy, **proxy_kwargs):
        # Keep a simple proxy manager mapping — suitable for most uses.
        if not hasattr(self, "proxy_managers"):
            self.proxy_managers = {}
        if proxy not in self.proxy_managers:
            self.proxy_managers[proxy] = PoolManager(
                num_pools=self._pool_connections,
                maxsize=self._pool_maxsize,
                ssl_context=self.ssl_context,
                **proxy_kwargs,
            )
        return self.proxy_managers[proxy]


def make_legacy_session() -> requests.Session:
    """Create a `requests.Session` that enables legacy server renegotiation
    where supported by the local OpenSSL/Python build. Falls back to a
    session with `verify=False` if SSL tweaks aren't possible.
    """
    sess = requests.Session()
    try:
        ctx = ssl.create_default_context()
        # Prefer the modern OP_LEGACY_SERVER_CONNECT flag (OpenSSL 3+ / Python)
        if hasattr(ssl, "OP_LEGACY_SERVER_CONNECT"):
            ctx.options |= ssl.OP_LEGACY_SERVER_CONNECT
        # Older builds may expose the older constant name
        elif hasattr(ssl, "OP_ALLOW_UNSAFE_LEGACY_RENEGOTIATION"):
            ctx.options |= ssl.OP_ALLOW_UNSAFE_LEGACY_RENEGOTIATION

        adapter = SSLContextAdapter(ssl_context=ctx)
        sess.mount("https://", adapter)
    except Exception:
        # As a last resort, disable certificate verification (less secure)
        sess.verify = False
    return sess


# Persistent session reused across all paginated ASMX calls.
ASMX_SESSION = make_legacy_session()


def fetch_asmx_page(page_number: int) -> tuple[list[dict], int]:
    """
    POST to the LoadMoreArticle ASMX endpoint.
    Returns (stubs, total_pages).
    """
    payload = {
        "siteid": ASMX_SITEID,
        "catecode": ASMX_CATECODE,
        "pagenumber": page_number,
        "pagesize": ASMX_PAGESIZE,
        "isparrent": ASMX_ISPARRENT,
    }

    def _parse_text(text: str) -> tuple[list[dict], int]:
        try:
            outer = json.loads(text)
            inner = json.loads(outer["d"])
        except (json.JSONDecodeError, KeyError) as e:
            raise ValueError(e)

        html_fragment = inner.get("listArticle", "")
        total_pages = int(inner.get("TotalPages", 1))
        stubs = parse_stubs_from_html(html_fragment)
        return stubs, total_pages

    # Primary path: persistent legacy-compatible session with connection pooling.
    try:
        resp = ASMX_SESSION.post(
            ASMX_URL, headers=ASMX_HEADERS, json=payload, timeout=15
        )
        resp.raise_for_status()
        stubs, total_pages = _parse_text(resp.text)
        print(
            f"  [Page {page_number:>2}] ASMX API → {len(stubs)} articles "
            f"(total pages: {total_pages})"
        )
        return stubs, total_pages

    except requests.RequestException as e:
        print(f"  ⚠  ASMX request failed (page {page_number}): {e}")
        # Fall through to curl fallback

    # Final fallback: use system `curl` to POST (with minimal headers)
    try:
        body = json.dumps(payload)
        curl_headers = [
            "-H",
            "Accept: application/json, text/javascript, */*; q=0.01",
            "-H",
            "Content-Type: application/json; charset=UTF-8",
            "-H",
            f"Referer: {NEWS_INDEX}",
            "-H",
            "X-Requested-With: XMLHttpRequest",
        ]
        cmd = ["curl", "-sS", "-X", "POST"] + curl_headers + ["--data", body, ASMX_URL]
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if proc.returncode != 0:
            print(f"  ⚠  curl fallback failed: {proc.stderr.strip()[:200]}")
            return [], 0

        stubs, total_pages = _parse_text(proc.stdout)
        print(
            f"  [Page {page_number:>2}] ASMX API (curl fallback) → {len(stubs)} articles "
            f"(total pages: {total_pages})"
        )
        return stubs, total_pages
    except Exception as e3:
        print(f"  ⚠  curl fallback parse failed (page {page_number}): {e3}")
        return [], 0


def scrape_all_pages() -> list[dict]:
    """
    Combine page-1 SSR + page 2..N ASMX to collect all article stubs.
    Respects MAX_ARTICLES cap if set.
    """
    print("[Depth 1] Collecting article URLs (all pages)…")

    all_stubs = scrape_page1()
    seen = {s["url"] for s in all_stubs}

    # Probe page 2 to discover total_pages
    stubs_p2, total_pages = fetch_asmx_page(2)
    for s in stubs_p2:
        if s["url"] not in seen:
            seen.add(s["url"])
            all_stubs.append(s)

    # Pages 3 … total_pages
    for page in range(3, total_pages + 1):
        if MAX_ARTICLES and len(all_stubs) >= MAX_ARTICLES:
            break
        time.sleep(REQUEST_DELAY)
        stubs, _ = fetch_asmx_page(page)
        for s in stubs:
            if s["url"] not in seen:
                seen.add(s["url"])
                all_stubs.append(s)

    if MAX_ARTICLES:
        all_stubs = all_stubs[:MAX_ARTICLES]

    print(f"\n  ✓  Total unique article URLs: {len(all_stubs)}")
    return all_stubs


# ── Content cleaner ───────────────────────────────────────────────────────────

_NOISE_LINE_RE = re.compile(
    r"""
    ^\s*-\s*$                       # empty bullet
    | ^\s*-\s*A[+\-]?\s*$          # font size controls
    | ^[-\s]*$                      # blank / only dashes
    | Photo\s+courtesy              # photo credit
    | —\s*Photo                     # alternate photo credit prefix
    | —\s*BIZHUB\s*$               # source attribution
    | ^\+\s*Load\s+more             # pagination artifact
    """,
    re.VERBOSE | re.IGNORECASE,
)

_STOP_MARKERS = [
    "- share:",
    "- tags",
    "## comments",
    "## see also",
    "see also",
    "abc123@",
]


def clean_body(plain_text: str) -> str:
    lines = plain_text.splitlines()

    # Find body start: line after "- A+"
    start = 0
    for i, line in enumerate(lines):
        if re.match(r"^\s*-\s*A\+\s*$", line):
            start = i + 1
            break

    # Collect until stop marker
    body_lines: list[str] = []
    for line in lines[start:]:
        if any(
            line.strip().lower().startswith(m) or m in line.strip().lower()
            for m in _STOP_MARKERS
        ):
            break
        body_lines.append(line.strip())

    # Drop noise, deduplicate, collapse blanks
    body_lines = [l for l in body_lines if not _NOISE_LINE_RE.match(l)]
    deduped: list[str] = []
    prev = None
    for line in body_lines:
        if line == prev and line != "":
            continue
        deduped.append(line)
        prev = line

    text = "\n".join(deduped)
    return re.sub(r"\n{3,}", "\n\n", text).strip()


# ── Metadata extractor ────────────────────────────────────────────────────────


def extract_metadata(raw: dict) -> dict:
    meta = raw.get("metadata", {})
    sd = raw.get("structured_data", [])
    news_sd = next((s for s in sd if s.get("@type") == "NewsArticle"), {})
    links = raw.get("content", {}).get("links", [])
    tags = [l["text"] for l in links if "/tags/" in l.get("href", "") and l.get("text")]
    return {
        "title": news_sd.get("headline") or meta.get("title", ""),
        "description": news_sd.get("description") or meta.get("description", ""),
        "published_date": news_sd.get("datePublished")
        or meta.get("published_date", ""),
        "author": meta.get("author", ""),
        "word_count": meta.get("word_count"),
        "tags": tags,
    }


# ── Depth 2: fetch + clean each article ──────────────────────────────────────


def fetch_and_clean(stub: dict) -> dict:
    raw = webclaw_scrape(stub["url"])
    if not raw:
        return {
            **stub,
            "body": None,
            "published_date": None,
            "description": None,
            "author": None,
            "tags": [],
            "word_count": None,
        }

    plain = raw.get("content", {}).get("plain_text", "")
    meta = extract_metadata(raw)
    return {
        "title": meta["title"] or stub["title"],
        "url": stub["url"],
        "published_date": meta["published_date"],
        "description": meta["description"],
        "author": meta["author"],
        "tags": meta["tags"],
        "word_count": meta["word_count"],
        "body": clean_body(plain),
    }


def fetch_all_articles(stubs: list[dict]) -> list[dict]:
    print(
        f"\n[Depth 2] Fetching + cleaning {len(stubs)} articles "
        f"(workers={MAX_WORKERS})…\n"
    )

    results: list[dict] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(fetch_and_clean, s): s["title"] for s in stubs}
        for i, future in enumerate(as_completed(futures), 1):
            title = futures[future]
            article = future.result()
            ok = "✓" if article.get("body") else "⚠ "
            words = len((article.get("body") or "").split())
            print(f"  [{i:>3}/{len(stubs)}] {ok} {title[:62]:<62} ({words:>4} words)")
            results.append(article)

    success = sum(1 for a in results if a.get("body"))
    print(f"\n  ✓  Clean articles: {success}/{len(results)}")
    return results


# ── Save ──────────────────────────────────────────────────────────────────────


def save(articles: list[dict]) -> None:
    payload = {
        "scraped_at": datetime.utcnow().isoformat() + "Z",
        "source": NEWS_INDEX,
        "count": len(articles),
        "articles": articles,
    }
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"\n✅  Saved → {OUTPUT_FILE}  ({len(articles)} articles)")


# ── Preview ───────────────────────────────────────────────────────────────────


def preview(articles: list[dict]) -> None:
    print("\n── Clean body preview (first 2 articles) " + "─" * 20)
    for a in articles[:2]:
        print(f"\n  Title   : {a['title']}")
        print(f"  Date    : {a['published_date']}")
        print(f"  Tags    : {', '.join(a['tags']) or '—'}")
        print(f"  Words   : {a['word_count']}")
        if a.get("body"):
            paras = [p for p in a["body"].split("\n\n") if p.strip()][:3]
            for p in paras:
                print(f"\n  ▸ {p[:200]}")
        print()


# ── Main ──────────────────────────────────────────────────────────────────────


def main():
    print("=" * 65)
    print("  webclaw Paginated News Extractor — bizhub.vietnamnews.vn")
    print("=" * 65 + "\n")

    stubs = scrape_all_pages()
    if not stubs:
        print("No articles found. Verify ASMX_CATECODE and ASMX_ISPARRENT in config.")
        return

    articles = fetch_all_articles(stubs)
    save(articles)
    preview(articles)


if __name__ == "__main__":
    main()
