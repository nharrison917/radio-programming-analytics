# scraper/artist_enrichment.py
# -*- coding: utf-8 -*-

import sqlite3
import requests
import time
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from scraper.config import (
    DB_PATH,
    ARTIST_ENRICHMENT_BATCH_SIZE,
    ARTIST_ENRICHMENT_CHUNK_SIZE,
    ARTIST_ENRICHMENT_COOLDOWN_SECONDS,
    ARTIST_ENRICHMENT_REQUEST_DELAY,
)
from scraper.enrichment import get_spotify_token


MAX_TRANSIENT_RETRIES = 3


# ---------------------
# Seeding
# ---------------------

def seed_canonical_artists():
    """Insert any artist IDs from successfully enriched tracks not yet in canonical_artists."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Ensure table exists regardless of whether init_db() has been called this session
    cur.execute("""
        CREATE TABLE IF NOT EXISTS canonical_artists (
            spotify_artist_id   TEXT PRIMARY KEY,
            artist_name         TEXT,
            earliest_release_year INTEGER,
            earliest_release_name TEXT,
            enrichment_status   TEXT DEFAULT 'PENDING',
            last_attempted_at   TEXT,
            attempt_count       INTEGER DEFAULT 0,
            created_at          TEXT
        )
    """)
    conn.commit()

    cur.execute("""
        INSERT OR IGNORE INTO canonical_artists (
            spotify_artist_id,
            artist_name,
            enrichment_status,
            created_at
        )
        SELECT DISTINCT
            ct.spotify_primary_artist_id,
            ct.spotify_primary_artist_name,
            'PENDING',
            DATETIME('now')
        FROM canonical_tracks ct
        WHERE ct.spotify_status = 'SUCCESS'
          AND ct.spotify_primary_artist_id IS NOT NULL
    """)

    seeded = cur.rowcount
    conn.commit()
    conn.close()

    logging.info(f"Artist seed: {seeded} new artist(s) added to canonical_artists")
    print(f"Artist seed: {seeded} new artist(s) added")
    return seeded


# ---------------------
# API helpers
# ---------------------

def _handle_rate_limit(response):
    """Read Retry-After, print resume time, raise RuntimeError to abort the run."""
    retry_after = int(response.headers.get("Retry-After", 60))
    now = datetime.now(ZoneInfo("America/New_York"))
    resume_time = now + timedelta(seconds=retry_after)
    print(f"\nRate limit detected ({retry_after}s). Aborting artist enrichment.")
    print(
        f"Resume after "
        f"{resume_time.strftime('%I:%M %p')} "
        f"on {resume_time.strftime('%A, %B %d, %Y')} (ET)\n"
    )
    raise RuntimeError("Rate limit exceeded.")


def _fetch_all_releases(artist_id, token, include_groups="album"):
    """Paginate through all releases for an artist.

    include_groups: comma-separated Spotify group types, e.g. "album" or "album,single".
    Raises RuntimeError on 429 (caller should abort the run).
    Raises requests.HTTPError on other non-200 responses (caller may retry).
    Returns (releases, page_count) where page_count is the number of API requests made.
    """
    releases = []
    offset = 0
    limit = 10
    page_count = 0

    while True:
        response = requests.get(
            f"https://api.spotify.com/v1/artists/{artist_id}/albums",
            headers={"Authorization": f"Bearer {token}"},
            params={"include_groups": include_groups, "market": "US", "limit": limit, "offset": offset}
        )
        page_count += 1
        time.sleep(ARTIST_ENRICHMENT_REQUEST_DELAY)  # proactive throttle per page

        if response.status_code == 429:
            _handle_rate_limit(response)  # raises RuntimeError

        if response.status_code != 200:
            logging.warning(
                f"Non-200 response for artist_id={artist_id}: "
                f"status={response.status_code} body={response.text[:200]}"
            )
            response.raise_for_status()  # raises HTTPError for backoff

        data = response.json()
        releases.extend(data.get("items", []))

        if data.get("next") is None:
            break

        offset += limit

    return releases, page_count


def _fetch_releases_with_backoff(artist_id, token, include_groups="album"):
    """Wrap _fetch_all_releases with exponential backoff for transient (non-429) errors.

    429s propagate immediately -- they abort the whole run.
    Other errors are retried up to MAX_TRANSIENT_RETRIES times with doubling delay.
    """
    delay = 2
    last_exc = None

    for attempt in range(MAX_TRANSIENT_RETRIES):
        try:
            return _fetch_all_releases(artist_id, token, include_groups)  # (releases, page_count)
        except RuntimeError:
            raise  # 429 -- do not retry, propagate to abort
        except Exception as exc:
            last_exc = exc
            if attempt < MAX_TRANSIENT_RETRIES - 1:
                logging.warning(
                    f"Transient error fetching artist_id={artist_id} "
                    f"(attempt {attempt + 1}/{MAX_TRANSIENT_RETRIES}): {exc} "
                    f"-- retrying in {delay}s"
                )
                time.sleep(delay)
                delay = min(delay * 2, 30)

    raise last_exc


# ---------------------
# Parsing
# ---------------------

def _parse_earliest_release(releases):
    """Return (year, name) for the earliest valid release in the list.

    Handles Spotify release_date precision: YYYY, YYYY-MM, YYYY-MM-DD.
    Years outside 1920 to current_year+1 are treated as implausible and skipped.
    Returns (None, None) if no valid year is found.
    """
    current_year = datetime.utcnow().year
    earliest_year = None
    earliest_name = None

    for release in releases:
        release_date = release.get("release_date")
        if not release_date:
            continue

        try:
            year = int(release_date.split("-")[0])
        except (ValueError, IndexError):
            continue

        if year < 1920 or year > current_year + 1:
            continue  # implausible -- skip without nulling (not writing to DB)

        if earliest_year is None or year < earliest_year:
            earliest_year = year
            earliest_name = release.get("name")

    return earliest_year, earliest_name


# ---------------------
# Main enrichment loop
# ---------------------

def enrich_artists(client_id, client_secret):
    token = get_spotify_token(client_id, client_secret)

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        SELECT spotify_artist_id, artist_name
        FROM canonical_artists
        WHERE enrichment_status = 'PENDING'
          AND (
              last_attempted_at IS NULL
              OR last_attempted_at < DATETIME('now', '-2 days')
          )
        LIMIT ?
    """, (ARTIST_ENRICHMENT_BATCH_SIZE,))

    rows = cur.fetchall()
    total = len(rows)
    enriched_count = 0
    failure_count = 0
    total_requests = 0
    rate_limit_abort = False

    print(f"Artists to process this run: {total}")

    try:
        for i in range(0, total, ARTIST_ENRICHMENT_CHUNK_SIZE):
            chunk = rows[i:i + ARTIST_ENRICHMENT_CHUNK_SIZE]
            print(f"\nProcessing artist chunk {i}--{i + len(chunk)}")

            for artist_id, artist_name in chunk:

                # Record the attempt before any network call
                cur.execute("""
                    UPDATE canonical_artists
                    SET last_attempted_at = DATETIME('now'),
                        attempt_count = COALESCE(attempt_count, 0) + 1
                    WHERE spotify_artist_id = ?
                """, (artist_id,))
                conn.commit()

                try:
                    releases, req_count = _fetch_releases_with_backoff(artist_id, token)
                    total_requests += req_count
                    if not releases:
                        logging.info(
                            f"No albums for artist_id={artist_id} ({artist_name})"
                            f" -- retrying with singles fallback"
                        )
                        releases, req_count = _fetch_releases_with_backoff(
                            artist_id, token, include_groups="album,single"
                        )
                        total_requests += req_count
                except RuntimeError:
                    raise  # 429 -- propagate to outer handler
                except Exception as exc:
                    logging.warning(
                        f"Failed to fetch releases for artist_id={artist_id} "
                        f"({artist_name}): {exc}"
                    )
                    cur.execute("""
                        UPDATE canonical_artists
                        SET enrichment_status = 'FAILED'
                        WHERE spotify_artist_id = ?
                    """, (artist_id,))
                    conn.commit()
                    failure_count += 1
                    continue

                if not releases:
                    logging.info(
                        f"No releases found for artist_id={artist_id} ({artist_name})"
                        f" (albums + singles)"
                    )
                    cur.execute("""
                        UPDATE canonical_artists
                        SET enrichment_status = 'FAILED'
                        WHERE spotify_artist_id = ?
                    """, (artist_id,))
                    conn.commit()
                    failure_count += 1
                    continue

                earliest_year, earliest_name = _parse_earliest_release(releases)

                if earliest_year is None:
                    logging.warning(
                        f"No valid release year for artist_id={artist_id} ({artist_name})"
                        f" -- {len(releases)} item(s) returned, none with plausible year"
                    )
                    cur.execute("""
                        UPDATE canonical_artists
                        SET enrichment_status = 'FAILED'
                        WHERE spotify_artist_id = ?
                    """, (artist_id,))
                    conn.commit()
                    failure_count += 1
                    continue

                cur.execute("""
                    UPDATE canonical_artists
                    SET earliest_release_year = ?,
                        earliest_release_name = ?,
                        enrichment_status = 'SUCCESS'
                    WHERE spotify_artist_id = ?
                """, (earliest_year, earliest_name, artist_id))
                conn.commit()
                enriched_count += 1
                logging.info(
                    f"Enriched artist_id={artist_id} ({artist_name}): "
                    f"earliest release '{earliest_name}' ({earliest_year})"
                )

            processed = min(i + ARTIST_ENRICHMENT_CHUNK_SIZE, total)
            print(f"Processed {processed}/{total}")

            if i + ARTIST_ENRICHMENT_CHUNK_SIZE < total:
                print(f"Cooling down {ARTIST_ENRICHMENT_COOLDOWN_SECONDS}s...")
                time.sleep(ARTIST_ENRICHMENT_COOLDOWN_SECONDS)

    except RuntimeError:
        print(f"Artist enrichment aborted due to rate limiting. API requests this run: {total_requests}")
        conn.close()
        return {
            "artists_enriched": enriched_count,
            "artist_failures": failure_count,
            "artist_rate_limit_abort": True,
            "api_requests": total_requests,
        }

    print(f"Artist enrichment complete. API requests this run: {total_requests}")
    conn.close()
    return {
        "artists_enriched": enriched_count,
        "artist_failures": failure_count,
        "artist_rate_limit_abort": rate_limit_abort,
        "api_requests": total_requests,
    }
