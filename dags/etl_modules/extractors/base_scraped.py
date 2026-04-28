from abc import ABC, abstractmethod


class ScrapedExtractor(ABC):
    """
    Base class for scraped sources that are not ticker-linked at ingest time.
    """

    @abstractmethod
    def extract(self) -> list[dict]:
        """
        Return normalized rows with:
            news_id, title, news_content, publish_date, source, source_url,
            source_type='scraped'
        """
