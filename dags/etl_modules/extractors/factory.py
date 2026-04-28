"""
Extractor factory — runs all registered extractors concurrently.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeoutError

from dags.etl_modules.extractors.bizhub import BizhubExtractor
from dags.etl_modules.extractors.kbs import KBSNewsExtractor
from dags.etl_modules.extractors.theinvestor import TheInvestorExtractor
from dags.etl_modules.extractors.vci import VCINewsExtractor

logger = logging.getLogger(__name__)

TICKER_LINKED_EXTRACTOR_CLASSES = []
SCRAPED_EXTRACTOR_CLASSES = [BizhubExtractor, TheInvestorExtractor]
EXTRACTOR_TIMEOUT_SECS = 1800


def run_all_extractors(tickers: list[dict]) -> tuple[list[dict], list[dict]]:
    """
    Fetch news from all registered sources.

    - One extractor failing does not block the others.
    - Returns separate collections for ticker-linked and scraped records.
    """
    logger.info("Extractor factory: %s active tickers provided", len(tickers))

    ticker_linked_results: list[dict] = []
    scraped_results: list[dict] = []

    def safe_extract(cls):
        try:
            if cls in TICKER_LINKED_EXTRACTOR_CLASSES:
                return cls(tickers=tickers).extract()
            return cls().extract()
        except Exception as exc:
            logger.warning("%s failed: %s", cls.__name__, exc)
            return []

    all_classes = [*TICKER_LINKED_EXTRACTOR_CLASSES, *SCRAPED_EXTRACTOR_CLASSES]
    with ThreadPoolExecutor(max_workers=len(all_classes)) as pool:
        futures = {pool.submit(safe_extract, cls): cls for cls in all_classes}
        for future, cls in futures.items():
            try:
                data = future.result(timeout=EXTRACTOR_TIMEOUT_SECS)
                if cls in TICKER_LINKED_EXTRACTOR_CLASSES:
                    ticker_linked_results.extend(data)
                else:
                    scraped_results.extend(data)
                logger.info("%s: %s records returned", cls.__name__, len(data))
            except FuturesTimeoutError:
                logger.warning(
                    "%s timed out after %ss — skipping",
                    cls.__name__,
                    EXTRACTOR_TIMEOUT_SECS,
                )

    if not ticker_linked_results and not scraped_results:
        raise RuntimeError(
            "All news extractors failed or returned empty. "
            "Check extractor logs for details."
        )

    return ticker_linked_results, scraped_results
