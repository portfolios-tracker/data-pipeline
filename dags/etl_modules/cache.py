import os
import logging
import pickle
import redis
import functools
import hashlib
import pandas as pd
from datetime import timedelta

# Configuration
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))

# Initialize Redis client
try:
    redis_client = redis.Redis(
        host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=False
    )
    # Test connection
    redis_client.ping()
    logging.info(f"Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
except Exception as e:
    logging.warning(f"Failed to connect to Redis: {e}. Caching will be disabled.")
    redis_client = None


def get_cache_key(func_name, args, kwargs):
    """Generate a unique cache key based on function name and arguments."""
    # Create a string representation of args and kwargs
    arg_str = str(args) + str(sorted(kwargs.items()))
    # Hash it to keep the key short and safe
    arg_hash = hashlib.md5(arg_str.encode("utf-8")).hexdigest()
    return f"cache:{func_name}:{arg_hash}"


def cached_data(ttl_seconds=3600):
    """
    Decorator to cache function results in Redis.
    Suitable for functions returning pandas DataFrames or serializable objects.
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if not redis_client:
                return func(*args, **kwargs)

            key = get_cache_key(func.__name__, args, kwargs)

            try:
                # Try to get from cache
                cached_bytes = redis_client.get(key)
                if cached_bytes:
                    logging.info(f"Cache HIT for {func.__name__}")
                    return pickle.loads(cached_bytes)
            except Exception as e:
                logging.warning(f"Error reading from cache for {func.__name__}: {e}")

            # Execute function
            result = func(*args, **kwargs)

            try:
                # Save to cache
                # Only cache if result is not empty (for DataFrames)
                if isinstance(result, pd.DataFrame) and result.empty:
                    pass  # Don't cache empty results? Or maybe yes? Let's avoid caching failures.
                elif result is None:
                    pass
                else:
                    redis_client.setex(key, timedelta(seconds=ttl_seconds), pickle.dumps(result))
                    logging.debug(f"Cache SET for {func.__name__}")
            except Exception as e:
                logging.warning(f"Error writing to cache for {func.__name__}: {e}")

            return result

        return wrapper

    return decorator
