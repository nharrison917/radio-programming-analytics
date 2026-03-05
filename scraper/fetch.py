import requests
import time
from urllib.parse import urlparse

from scraper.config import (
    HEADERS,
    REQUEST_TIMEOUT,
    MAX_RETRIES,
    RETRY_BACKOFF,
    DISALLOWED_PATH_FRAGMENTS,
)


def is_allowed_url(url: str) -> bool:
    parsed = urlparse(url)

    for fragment in DISALLOWED_PATH_FRAGMENTS:
        if fragment in parsed.path:
            return False

    return True


def fetch_url(url: str, params=None) -> str:
    if not is_allowed_url(url):
        raise ValueError(f"URL disallowed by local rules: {url}")

    attempt = 0

    while attempt < MAX_RETRIES:
        try:
            response = requests.get(
                url,
                params=params,
                headers=HEADERS,
                timeout=REQUEST_TIMEOUT,
            )

            response.raise_for_status()
            return response.text

        except Exception as e:
            attempt += 1
            wait = RETRY_BACKOFF * attempt
            print(f"Fetch failed (attempt {attempt}/{MAX_RETRIES}): {e}. Sleeping {wait}s")
            time.sleep(wait)

    raise RuntimeError(f"Failed to fetch {url} after {MAX_RETRIES} attempts")