from __future__ import annotations

from urllib.parse import urljoin

import requests

from dags.etl_modules.extractors.base_ticker_linked import TickerLinkedExtractor

_KBS_BASE = "https://kbbuddywts.kbsec.com.vn"
_KBS_STOCK_INFO_NEWS_URL = f"{_KBS_BASE}/iis-server/investment/stockinfo/news"
_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "User-Agent": "Mozilla/5.0",
}


class KBSNewsExtractor(TickerLinkedExtractor):
    @property
    def source_name(self) -> str:
        return "kbs"

    def fetch_for_ticker(self, symbol: str) -> list[dict]:
        response = requests.get(
            f"{_KBS_STOCK_INFO_NEWS_URL}/{symbol}",
            params={"l": 1, "p": 1, "s": 20},
            headers=_HEADERS,
            timeout=15,
        )
        response.raise_for_status()
        items = response.json() or []
        if not isinstance(items, list):
            return []

        rows: list[dict] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            news_id = item.get("ArticleID") or item.get("article_id") or item.get("id")
            if news_id is None:
                continue

            raw_url = str(item.get("URL") or item.get("url") or "").strip()
            source_url = urljoin(_KBS_BASE, raw_url) if raw_url else ""

            rows.append(
                {
                    "news_id": int(news_id),
                    "title": str(item.get("Title") or item.get("title") or "").strip(),
                    "news_content": str(
                        item.get("Head")
                        or item.get("head")
                        or item.get("Description")
                        or item.get("description")
                        or item.get("Content")
                        or item.get("content")
                        or ""
                    ).strip(),
                    "publish_date": str(
                        item.get("PublishTime")
                        or item.get("publish_time")
                        or item.get("published_at")
                        or item.get("publishedAt")
                        or ""
                    ).strip(),
                    "source_url": source_url,
                }
            )

        return rows
