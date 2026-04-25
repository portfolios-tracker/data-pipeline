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
