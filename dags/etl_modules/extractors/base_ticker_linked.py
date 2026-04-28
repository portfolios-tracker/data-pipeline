from __future__ import annotations

import logging
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class TickerLinkedExtractor(ABC):
    """
    Base class for providers that fetch news per ticker.

    Providers return normalized article fields while this base class attaches the
    ticker-linked metadata (asset_id, source, source_type).
    """

    def __init__(self, tickers: list[dict]) -> None:
        self.tickers = tickers

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Return the provider source identifier."""

    @abstractmethod
    def fetch_for_ticker(self, symbol: str) -> list[dict]:
        """
        Return normalized rows for one ticker with keys:
            news_id, title, news_content, publish_date, source_url
        """

    def extract(self) -> list[dict]:
        records: list[dict] = []
        for ticker in self.tickers:
            symbol = str(ticker.get("symbol") or "").strip().upper()
            asset_id = ticker.get("asset_id")
            if not symbol or not asset_id:
                continue
            try:
                rows = self.fetch_for_ticker(symbol)
                for row in rows:
                    row["asset_id"] = str(asset_id)
                    row["source"] = self.source_name
                    row["source_type"] = "ticker_linked"
                records.extend(rows)
            except Exception as exc:
                logger.warning("[%s] Failed to fetch %s: %s", self.source_name, symbol, exc)
        return records
