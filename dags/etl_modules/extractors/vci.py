from __future__ import annotations

from datetime import timezone

import pandas as pd

from dags.etl_modules.extractors.base_ticker_linked import TickerLinkedExtractor
from dags.etl_modules.vci_provider import _graphql


class VCINewsExtractor(TickerLinkedExtractor):
    _QUERY = """
    query Query($ticker: String!, $lang: String!) {
      News(ticker: $ticker, langCode: $lang) {
        id
        newsTitle
        newsSourceLink
        publicDate
        newsShortContent
        newsFullContent
      }
    }
    """

    @property
    def source_name(self) -> str:
        return "vci"

    def fetch_for_ticker(self, symbol: str) -> list[dict]:
        data = _graphql(self._QUERY, {"ticker": symbol, "lang": "vi"})
        raw_rows = data.get("News") or []
        if not raw_rows:
            return []

        rows: list[dict] = []
        for item in raw_rows:
            news_id = item.get("id") or item.get("newsId")
            if news_id is None:
                continue

            publish_date = pd.to_datetime(item.get("publicDate"), errors="coerce", utc=True)
            if pd.isna(publish_date):
                publish_date_str = pd.Timestamp.now(tz=timezone.utc).isoformat()
            else:
                publish_date_str = pd.Timestamp(publish_date).isoformat()

            news_content = str(item.get("newsFullContent") or item.get("newsShortContent") or "").strip()

            rows.append(
                {
                    "news_id": int(news_id),
                    "title": str(item.get("newsTitle") or "").strip(),
                    "news_content": news_content,
                    "publish_date": publish_date_str,
                    "source_url": str(item.get("newsSourceLink") or "").strip(),
                }
            )
        return rows
