import pytest


@pytest.mark.unit
def test_bizhub_extractor_does_not_require_tickers_for_scraped_source():
    from dags.etl_modules.extractors.bizhub import BizhubExtractor

    extractor = BizhubExtractor()
    assert extractor is not None


@pytest.mark.unit
def test_run_all_extractors_returns_ticker_linked_and_scraped_lists(monkeypatch):
    from dags.etl_modules.extractors import factory

    class _TickerLinkedExtractor:
        def __init__(self, tickers):
            self.tickers = tickers

        def extract(self):
            return [{"news_id": 1, "source_type": "ticker_linked"}]

    class _ScrapedExtractor:
        def __init__(self):
            pass

        def extract(self):
            return [{"news_id": 2, "source_type": "scraped"}]

    monkeypatch.setattr(factory, "TICKER_LINKED_EXTRACTOR_CLASSES", [_TickerLinkedExtractor])
    monkeypatch.setattr(factory, "SCRAPED_EXTRACTOR_CLASSES", [_ScrapedExtractor])

    ticker_linked, scraped = factory.run_all_extractors(
        tickers=[{"symbol": "HPG", "asset_id": "asset-1"}]
    )

    assert ticker_linked == [{"news_id": 1, "source_type": "ticker_linked"}]
    assert scraped == [{"news_id": 2, "source_type": "scraped"}]


@pytest.mark.unit
def test_kbs_news_extractor_normalizes_payload_to_canonical_fields(monkeypatch):
    from dags.etl_modules.extractors.kbs import KBSNewsExtractor

    class _Response:
        def raise_for_status(self):
            return None

        def json(self):
            return [
                {
                    "ArticleID": 1433135,
                    "Title": "HPG board resolution",
                    "Head": "Headline summary",
                    "PublishTime": "2026-04-24T10:05:46",
                    "URL": "/2026/04/hpg-board-resolution-1433135.htm",
                }
            ]

    def _fake_get(*args, **kwargs):
        return _Response()

    monkeypatch.setattr("dags.etl_modules.extractors.kbs.requests.get", _fake_get)

    extractor = KBSNewsExtractor(
        tickers=[{"symbol": "HPG", "asset_id": "00bfae0b-5400-4130-b2ee-f11a7c74fd45"}]
    )
    rows = extractor.extract()

    assert rows == [
        {
            "asset_id": "00bfae0b-5400-4130-b2ee-f11a7c74fd45",
            "news_id": 1433135,
            "title": "HPG board resolution",
            "news_content": "Headline summary",
            "publish_date": "2026-04-24T10:05:46",
            "source": "kbs",
            "source_url": "https://kbbuddywts.kbsec.com.vn/2026/04/hpg-board-resolution-1433135.htm",
            "source_type": "ticker_linked",
        }
    ]
