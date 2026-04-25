import os
import psycopg2

SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")

def truncate_text(text: str, max_chars: int = 1000) -> str:
    """Truncates text to a maximum number of characters and appends '...' if truncated."""
    if not text:
        return ""
    if len(text) > max_chars:
        return text[:max_chars] + "..."
    return text

def get_gemini_api_key() -> str | None:
    if not SUPABASE_DB_URL:
        return None
    conn = psycopg2.connect(SUPABASE_DB_URL)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT public.fetch_secret_by_name('GEMINI_API_KEY')")
            row = cur.fetchone()
            return row[0] if row else None
    finally:
        conn.close()

def chunk_text(text: str, chunk_size: int = 4000, chunk_overlap: int = 500) -> list[str]:
    """
    Splits text into chunks of maximum `chunk_size` characters, with `chunk_overlap`.
    Tries to split on sentence boundaries if possible.
    """
    if not text:
        return []
    
    chunks = []
    start = 0
    text_len = len(text)
    
    while start < text_len:
        end = start + chunk_size
        
        if end < text_len:
            # Look for a natural break in the last 20% of the window
            search_start = max(start, end - int(chunk_size * 0.2))
            last_period = text.rfind('.', search_start, end)
            last_newline = text.rfind('\n', search_start, end)
            
            break_point = max(last_period, last_newline)
            if break_point != -1:
                end = break_point + 1
            else:
                last_space = text.rfind(' ', search_start, end)
                if last_space != -1:
                    end = last_space
        
        chunks.append(text[start:end].strip())
        
        if end >= text_len:
            break
            
        start = end - chunk_overlap
        
    return chunks
