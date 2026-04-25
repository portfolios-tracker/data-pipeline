import pytest
from dags.etl_modules.gemini_helpers import truncate_text

def test_truncate_text():
    short_text = "This is short."
    assert truncate_text(short_text, 1000) == short_text

    long_text = "A" * 1500
    truncated = truncate_text(long_text, 1000)
    assert len(truncated) == 1003
    assert truncated.endswith("...")

from dags.etl_modules.gemini_helpers import chunk_text

def test_chunk_text():
    text = "Word. " * 1000  # 6000 characters
    
    # 4000 char size, 500 char overlap
    chunks = chunk_text(text, chunk_size=4000, chunk_overlap=500)
    
    assert len(chunks) == 2
    assert len(chunks[0]) <= 4000
    assert len(chunks[1]) <= 4000
    
    # Check overlap (chunks[1] should start with text from end of chunks[0])
    assert chunks[0].startswith("Word.")
    assert chunks[1].endswith("Word.")
