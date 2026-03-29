# -*- coding: utf-8 -*-
"""
Stage 2 backfill: fetch spotify_isrc and spotify_album_type for all
SUCCESS canonical_tracks that predate Phase Two enrichment.

Uses the Spotify batch tracks endpoint (up to 50 IDs per call), so the
full catalog of ~2,500 tracks costs roughly 50 API calls.

Rate limiting mirrors enrichment.py:
  - 0.5s proactive sleep after every batch call
  - Hard abort on 429 with Retry-After surfaced to the user
"""

import sqlite3
import requests
import time
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from scraper.config import DB_PATH, SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET
from scraper.enrichment import get_spotify_token

CHUNK_SIZE = 50        # tracks per progress-reporting chunk
CALL_SLEEP = 0.3      # seconds between individual track calls (proactive throttle)
COOLDOWN_SECONDS = 10 # seconds between chunks (mirrors enrichment.py)

log = logging.getLogger(__name__)


def _fetch_single_track(spotify_id, token):
    """Fetch one track from Spotify.

    Returns the track object, or None if not found.
    Raises RuntimeError on 429.
    """
    response = requests.get(
        f"https://api.spotify.com/v1/tracks/{spotify_id}",
        headers={"Authorization": f"Bearer {token}"}
    )

    time.sleep(CALL_SLEEP)

    if response.status_code == 429:
        retry_after = int(response.headers.get("Retry-After", 60))
        now = datetime.now(ZoneInfo("America/New_York"))
        resume_time = now + timedelta(seconds=retry_after)
        print(
            f"\nRate limit hit ({retry_after}s). Aborting backfill.\n"
            f"Resume after {resume_time.strftime('%I:%M %p')} "
            f"on {resume_time.strftime('%A, %B %d, %Y')} (ET)\n"
        )
        raise RuntimeError("Rate limit exceeded.")

    if response.status_code == 404:
        return None

    response.raise_for_status()
    return response.json()


def backfill_spotify_meta():
    """Backfill spotify_isrc and spotify_album_type for existing SUCCESS records.

    Idempotent: skips records where spotify_isrc is already populated.
    Records where Spotify returns a null ISRC are written as empty string
    so they are not re-attempted on the next run.
    """
    token = get_spotify_token(SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET)

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        SELECT canonical_id, spotify_id
        FROM canonical_tracks
        WHERE spotify_status = 'SUCCESS'
          AND spotify_id IS NOT NULL
          AND spotify_isrc IS NULL
        ORDER BY canonical_id
    """)
    rows = cur.fetchall()
    total = len(rows)

    print(f"=== Spotify Meta Backfill ===")
    print(f"  Records to backfill: {total}")

    if total == 0:
        print("  Nothing to do.")
        conn.close()
        return {"backfilled": 0, "no_isrc": 0, "rate_limit_abort": False}

    backfilled = 0
    no_isrc = 0

    try:
        for chunk_start in range(0, total, CHUNK_SIZE):
            chunk = rows[chunk_start: chunk_start + CHUNK_SIZE]
            chunk_num = chunk_start // CHUNK_SIZE + 1
            chunk_total = (total + CHUNK_SIZE - 1) // CHUNK_SIZE
            print(f"  Chunk {chunk_num}/{chunk_total} ({chunk_start + 1}-{chunk_start + len(chunk)} of {total})...")

            for canonical_id, spotify_id in chunk:
                track = _fetch_single_track(spotify_id, token)

                if track is None:
                    # Track no longer exists on Spotify; write empty strings
                    # so this record is not re-attempted
                    cur.execute("""
                        UPDATE canonical_tracks
                        SET spotify_isrc = '', spotify_album_type = ''
                        WHERE canonical_id = ?
                    """, (canonical_id,))
                    no_isrc += 1
                    backfilled += 1
                    continue

                isrc = track.get("external_ids", {}).get("isrc") or ""
                album_type = track.get("album", {}).get("album_type") or ""

                if not isrc:
                    no_isrc += 1

                cur.execute("""
                    UPDATE canonical_tracks
                    SET spotify_isrc = ?,
                        spotify_album_type = ?
                    WHERE canonical_id = ?
                """, (isrc, album_type, canonical_id))
                backfilled += 1

            conn.commit()

            if chunk_start + CHUNK_SIZE < total:
                print(f"  Cooldown {COOLDOWN_SECONDS}s...")
                time.sleep(COOLDOWN_SECONDS)

    except RuntimeError:
        conn.close()
        return {"backfilled": backfilled, "no_isrc": no_isrc, "rate_limit_abort": True}

    conn.close()

    print(f"  Done. Backfilled: {backfilled}, No ISRC from Spotify: {no_isrc}")
    return {"backfilled": backfilled, "no_isrc": no_isrc, "rate_limit_abort": False}


if __name__ == "__main__":
    backfill_spotify_meta()
