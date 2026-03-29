# -*- coding: utf-8 -*-
"""
Stage 3: MusicBrainz lookup for tracks with unreliable Spotify album years.

Targets canonical_tracks where:
  - spotify_album_type = "compilation"
  - OR spotify_album_name contains remaster/deluxe signals

Uses the MusicBrainz ISRC endpoint to retrieve first-release-date, which
reflects the original recording release regardless of later compilations or
remasters. Takes the earliest valid year across all recordings linked to the ISRC.

Rate limit: 1 request/second (MusicBrainz requirement).
User-Agent header is required by MusicBrainz -- requests without it are rejected.
"""

import sqlite3
import requests
import time
import logging
from datetime import datetime
from pathlib import Path

from scraper.config import DB_PATH, SCRAPER_CONTACT

MB_CALL_SLEEP = 1.1          # seconds between calls (MB rate limit: 1 req/sec)
CHUNK_SIZE = 50              # records per progress-reporting chunk
RETRY_AFTER_DAYS = 7         # days before retrying a FAILED record

REMASTER_SIGNALS = ["remaster", "deluxe", "anniversary", "expanded", "edition"]  # lowercased for matching

MB_USER_AGENT = f"radio-scraper/1.0 ({SCRAPER_CONTACT})"

log = logging.getLogger(__name__)


def _is_remaster(album_name):
    """Return True if album_name contains a remaster/deluxe signal."""
    if not album_name:
        return False
    lower = album_name.lower()
    return any(signal in lower for signal in REMASTER_SIGNALS)


def _earliest_valid_year(recordings):
    """Extract the earliest plausible first-release-year across all recordings.

    MusicBrainz first-release-date may be YYYY, YYYY-MM, or YYYY-MM-DD.
    Returns int year or None if nothing valid found.
    """
    current_year = datetime.utcnow().year
    years = []
    for rec in recordings:
        frd = rec.get("first-release-date", "")
        if not frd:
            continue
        try:
            year = int(str(frd)[:4])
        except (ValueError, TypeError):
            continue
        if 1920 <= year <= current_year + 1:
            years.append(year)
    return min(years) if years else None


def _lookup_isrc(isrc):
    """Query MusicBrainz for an ISRC.

    Returns (year, status) where:
      year   -- int or None
      status -- "SUCCESS", "FAILED", or "NO_ISRC"
    Raises RuntimeError on unexpected HTTP errors.
    """
    if not isrc:
        return None, "NO_ISRC"

    response = requests.get(
        f"https://musicbrainz.org/ws/2/isrc/{isrc}",
        params={"fmt": "json"},
        headers={"User-Agent": MB_USER_AGENT}
    )

    time.sleep(MB_CALL_SLEEP)

    if response.status_code == 404:
        return None, "FAILED"

    if response.status_code == 503:
        # MB overload -- treat as temporary failure, do not abort
        log.warning(f"MusicBrainz 503 for ISRC {isrc} -- skipping this record")
        return None, "FAILED"

    if response.status_code != 200:
        raise RuntimeError(
            f"MusicBrainz unexpected status {response.status_code} for ISRC {isrc}"
        )

    recordings = response.json().get("recordings", [])
    year = _earliest_valid_year(recordings)

    if year is None:
        return None, "FAILED"

    return year, "SUCCESS"


def run_mb_enrichment():
    """Look up MusicBrainz first-release-year for compilation/remaster tracks.

    Idempotent: skips records already marked SUCCESS or SKIPPED, and records
    marked FAILED that were attempted within the last RETRY_AFTER_DAYS days.
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute(f"""
        SELECT canonical_id, display_artist, display_title,
               spotify_album_release_year, spotify_album_name,
               spotify_album_type, spotify_isrc
        FROM canonical_tracks
        WHERE spotify_status = 'SUCCESS'
          AND spotify_isrc IS NOT NULL
          AND spotify_isrc != ''
          AND (
              spotify_album_type = 'compilation'
              OR LOWER(spotify_album_name) LIKE '%remaster%'
              OR LOWER(spotify_album_name) LIKE '%deluxe%'
              OR LOWER(spotify_album_name) LIKE '%anniversary%'
              OR LOWER(spotify_album_name) LIKE '%expanded%'
              OR LOWER(spotify_album_name) LIKE '%edition%'
          )
          AND (
              mb_lookup_status IS NULL
              OR (
                  mb_lookup_status = 'FAILED'
                  AND mb_looked_up_at < DATETIME('now', '-{RETRY_AFTER_DAYS} days')
              )
          )
        ORDER BY canonical_id
    """)

    rows = cur.fetchall()
    total = len(rows)

    print("=== MusicBrainz Year Enrichment ===")
    print(f"  Eligible records : {total}")
    print(f"  MB call sleep    : {MB_CALL_SLEEP}s")
    print(f"  Est. duration    : ~{int(total * MB_CALL_SLEEP / 60)} min")
    print()

    if total == 0:
        print("  Nothing to do.")
        conn.close()
        return {"success": 0, "failed": 0, "no_isrc": 0}

    success = 0
    failed = 0
    no_isrc = 0
    corrections = []   # (artist, title, old_year, new_year) for summary

    try:
        for chunk_start in range(0, total, CHUNK_SIZE):
            chunk = rows[chunk_start: chunk_start + CHUNK_SIZE]
            chunk_num = chunk_start // CHUNK_SIZE + 1
            chunk_total = (total + CHUNK_SIZE - 1) // CHUNK_SIZE
            print(
                f"  Chunk {chunk_num}/{chunk_total} "
                f"({chunk_start + 1}-{chunk_start + len(chunk)} of {total})..."
            )

            for (canonical_id, artist, title, old_year,
                 album_name, album_type, isrc) in chunk:

                year, status = _lookup_isrc(isrc)

                cur.execute("""
                    UPDATE canonical_tracks
                    SET mb_first_release_year = ?,
                        mb_lookup_status = ?,
                        mb_looked_up_at = DATETIME('now')
                    WHERE canonical_id = ?
                """, (year, status, canonical_id))

                if status == "SUCCESS":
                    success += 1
                    if year and old_year and year != old_year:
                        corrections.append((artist, title, old_year, year))
                elif status == "NO_ISRC":
                    no_isrc += 1
                else:
                    failed += 1

            conn.commit()

    except RuntimeError as e:
        log.error(f"MB enrichment aborted: {e}")
        conn.close()
        _print_summary(success, failed, no_isrc, corrections, aborted=True)
        return {"success": success, "failed": failed, "no_isrc": no_isrc,
                "aborted": True}

    conn.close()
    _print_summary(success, failed, no_isrc, corrections, aborted=False)
    return {"success": success, "failed": failed, "no_isrc": no_isrc,
            "aborted": False}


def _print_summary(success, failed, no_isrc, corrections, aborted):
    total = success + failed + no_isrc
    print()
    print(f"  --- Summary {'(ABORTED)' if aborted else ''} ---")
    print(f"  Looked up : {total}")
    print(f"  SUCCESS   : {success}")
    print(f"  FAILED    : {failed}")
    print(f"  NO_ISRC   : {no_isrc}")
    print(f"  Year corrections: {len(corrections)}")

    if corrections:
        corrections.sort(key=lambda x: abs(x[2] - x[3]), reverse=True)
        print()
        print(f"  Top corrections (largest year shift first):")
        for artist, title, old, new in corrections[:15]:
            print(f"    {artist} - {title}: {old} -> {new}  (shift={abs(old-new)}yr)")


if __name__ == "__main__":
    run_mb_enrichment()
