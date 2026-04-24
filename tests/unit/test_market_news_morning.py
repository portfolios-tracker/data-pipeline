import pytest
from dags.market_news_morning import truncate_text

def test_truncate_text():
    short_text = "This is short."
    assert truncate_text(short_text, 1000) == short_text
    
    long_text = "A" * 1500
    truncated = truncate_text(long_text, 1000)
    assert len(truncated) == 1003  # 1000 chars + "..."
    assert truncated.endswith("...")
