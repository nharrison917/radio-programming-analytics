# -*- coding: utf-8 -*-
"""
MusicBrainz year enrichment for canonical_tracks.

Two lookup methods, stored separately so both raw results are auditable:

  mb_isrc_year        -- year from MusicBrainz ISRC endpoint (version-specific
                         but precise; 404 if the ISRC is not in MB)
  mb_title_artist_year -- year from MusicBrainz recording text search filtered
                         to studio Album/Single release-groups only (broader
                         coverage, less precise)

best_year resolution (applied at query time, not stored):
  manual_year_override > min(mb_isrc_year, mb_title_artist_year if < spotify)
  > spotify_album_release_year

Eligibility: all spotify_status = 'SUCCESS' tracks (expanded from Phase Two
original which targeted compilations and remaster-flagged albums only).

Rate limit: 1 request/second (MusicBrainz requirement).
User-Agent header is required -- requests without it are rejected.
"""

import sqlite3
import requests
import time
import logging
from datetime import datetime
from urllib.parse import quote

from rapidfuzz import fuzz

from scraper.config import DB_PATH, SCRAPER_CONTACT

MB_CALL_SLEEP = 1.1          # seconds between every API call (MB rate limit: 1/sec)
CHUNK_SIZE = 50              # records per progress-reporting chunk
RETRY_AFTER_DAYS = 7        # days before retrying a FAILED record
FUZZY_THRESHOLD = 88        # minimum token_set_ratio for title and artist match

MB_USER_AGENT = f"radio-scraper/1.0 ({SCRAPER_CONTACT})"

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# ISRC lookup
# ---------------------------------------------------------------------------

def _earliest_valid_year(recordings):
    """Return the earliest plausible year across a list of MB recording objects.

    first-release-date may be YYYY, YYYY-MM, or YYYY-MM-DD.
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
    """Query MusicBrainz ISRC endpoint.

    Returns (year, status):
      year   -- int or None
      status -- 'SUCCESS', 'FAILED', or 'NO_ISRC'

    Always sleeps MB_CALL_SLEEP before returning (rate limit).
    Raises RuntimeError on unexpected HTTP status codes.
    """
    if not isrc:
        return None, "NO_ISRC"

    isrc = isrc.upper()

    response = requests.get(
        f"https://musicbrainz.org/ws/2/isrc/{isrc}",
        params={"fmt": "json"},
        headers={"User-Agent": MB_USER_AGENT},
    )
    time.sleep(MB_CALL_SLEEP)

    if response.status_code == 404:
        return None, "FAILED"

    if response.status_code == 503:
        log.warning(f"MusicBrainz 503 for ISRC {isrc} -- skipping")
        return None, "FAILED"

    if response.status_code != 200:
        raise RuntimeError(
            f"MusicBrainz unexpected status {response.status_code} for ISRC {isrc}"
        )

    recordings = response.json().get("recordings", [])
    year = _earliest_valid_year(recordings)
    return (year, "SUCCESS") if year is not None else (None, "FAILED")


# ---------------------------------------------------------------------------
# Title/artist search
# ---------------------------------------------------------------------------

def _clean_secondary_types(secondary):
    """Normalise secondary-types list, which may contain strings or dicts."""
    result = []
    for item in secondary:
        if isinstance(item, dict):
            result.append(item.get("name", ""))
        else:
            result.append(str(item))
    return result


def _lookup_title_artist(artist, title):
    """Query MusicBrainz recording search with release-group type filtering.

    Filters to recordings whose release-groups have:
      primary-type in ('Album', 'Single') AND secondary-types == []
    This excludes compilations, live albums, remixes, etc.

    Returns (year, status):
      year   -- int or None
      status -- 'SUCCESS' or 'FAILED'

    Always sleeps MB_CALL_SLEEP before returning (rate limit).
    Raises RuntimeError on unexpected HTTP status codes.
    """
    # Wrap field values in Lucene quotes so special characters (-,&,(),etc.)
    # in artist/title strings are treated as literals, not query operators.
    safe_artist = quote(artist.replace('"', ''))
    safe_title  = quote(title.replace('"', ''))
    url = (
        "https://musicbrainz.org/ws/2/recording"
        f'?query=artist:"{safe_artist}"+AND+recording:"{safe_title}"'
        "&fmt=json&limit=25&inc=releases+release-groups"
    )
    response = requests.get(url, headers={"User-Agent": MB_USER_AGENT})
    time.sleep(MB_CALL_SLEEP)

    if response.status_code == 503:
        log.warning(f"MusicBrainz 503 for title/artist search: {artist} - {title}")
        return None, "FAILED"

    if response.status_code != 200:
        raise RuntimeError(
            f"MusicBrainz unexpected status {response.status_code} "
            f"for title/artist search: {artist} - {title}"
        )

    current_year = datetime.utcnow().year
    recordings = response.json().get("recordings", [])
    candidates = []

    for rec in recordings:
        rec_title = rec.get("title", "")
        rec_artist = " ".join(
            c.get("name", "") for c in rec.get("artist-credit", [])
            if isinstance(c, dict)
        )

        title_score = fuzz.token_set_ratio(title.lower(), rec_title.lower())
        artist_score = fuzz.token_set_ratio(artist.lower(), rec_artist.lower())
        if title_score < FUZZY_THRESHOLD or artist_score < FUZZY_THRESHOLD:
            continue

        for release in rec.get("releases", []):
            rg = release.get("release-group", {})
            primary = rg.get("primary-type", "")
            secondary = _clean_secondary_types(rg.get("secondary-types", []))

            if primary not in ("Album", "Single") or secondary:
                continue

            date = release.get("date", "") or rg.get("first-release-date", "")
            if not date or len(date) < 4:
                continue
            try:
                year = int(date[:4])
            except (ValueError, TypeError):
                continue
            if 1920 <= year <= current_year + 1:
                candidates.append(year)

    if candidates:
        return min(candidates), "SUCCESS"
    return None, "FAILED"


# ---------------------------------------------------------------------------
# Main enrichment run
# ---------------------------------------------------------------------------

def run_mb_enrichment():
    """Run MusicBrainz year enrichment for all SUCCESS canonical tracks.

    Two passes per record:
      1. ISRC lookup  -> mb_isrc_year + mb_lookup_status
      2. Title/artist -> mb_title_artist_year + mb_ta_status

    Each pass is independently idempotent: existing SUCCESS records are
    skipped; FAILED records are retried after RETRY_AFTER_DAYS days.
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # --- Pass 1: ISRC lookup ---
    cur.execute(f"""
        SELECT canonical_id, display_artist, display_title,
               spotify_album_release_year, spotify_isrc
        FROM canonical_tracks
        WHERE spotify_status = 'SUCCESS'
          AND (
              mb_lookup_status IS NULL
              OR (
                  mb_lookup_status = 'FAILED'
                  AND mb_looked_up_at < DATETIME('now', '-{RETRY_AFTER_DAYS} days')
              )
          )
        ORDER BY canonical_id
    """)
    isrc_rows = cur.fetchall()
    isrc_total = len(isrc_rows)

    print("=== MusicBrainz Year Enrichment ===")
    print()
    print(f"  Pass 1 -- ISRC lookup")
    print(f"  Eligible : {isrc_total}")

    isrc_success = 0
    isrc_failed = 0
    isrc_no_isrc = 0
    isrc_corrections = []

    if isrc_total == 0:
        print("  Nothing to do.")
    else:
        print(f"  Est. duration : ~{int(isrc_total * MB_CALL_SLEEP / 60)} min")
        print()
        try:
            for chunk_start in range(0, isrc_total, CHUNK_SIZE):
                chunk = isrc_rows[chunk_start: chunk_start + CHUNK_SIZE]
                chunk_num = chunk_start // CHUNK_SIZE + 1
                chunk_total = (isrc_total + CHUNK_SIZE - 1) // CHUNK_SIZE
                print(
                    f"  Chunk {chunk_num}/{chunk_total} "
                    f"({chunk_start + 1}-{chunk_start + len(chunk)} of {isrc_total})..."
                )
                for (cid, artist, title, old_year, isrc) in chunk:
                    year, status = _lookup_isrc(isrc)
                    cur.execute("""
                        UPDATE canonical_tracks
                        SET mb_isrc_year    = ?,
                            mb_lookup_status = ?,
                            mb_looked_up_at  = DATETIME('now')
                        WHERE canonical_id = ?
                    """, (year, status, cid))

                    if status == "SUCCESS":
                        isrc_success += 1
                        if year and old_year and year < old_year:
                            isrc_corrections.append((artist, title, old_year, year))
                    elif status == "NO_ISRC":
                        isrc_no_isrc += 1
                    else:
                        isrc_failed += 1

                conn.commit()

        except RuntimeError as e:
            log.error(f"MB ISRC enrichment aborted: {e}")
            conn.close()
            _print_pass_summary(
                "ISRC", isrc_success, isrc_failed, isrc_no_isrc,
                isrc_corrections, aborted=True
            )
            return _build_result(
                isrc_success, isrc_failed, isrc_no_isrc, 0, 0, aborted=True
            )

    _print_pass_summary(
        "ISRC", isrc_success, isrc_failed, isrc_no_isrc, isrc_corrections
    )

    # --- Pass 2: Title/artist search ---
    cur.execute(f"""
        SELECT canonical_id, display_artist, display_title,
               spotify_album_release_year
        FROM canonical_tracks
        WHERE spotify_status = 'SUCCESS'
          AND (
              mb_ta_status IS NULL
              OR (
                  mb_ta_status = 'FAILED'
                  AND mb_looked_up_at < DATETIME('now', '-{RETRY_AFTER_DAYS} days')
              )
          )
        ORDER BY canonical_id
    """)
    ta_rows = cur.fetchall()
    ta_total = len(ta_rows)

    print()
    print(f"  Pass 2 -- Title/artist search")
    print(f"  Eligible : {ta_total}")

    ta_success = 0
    ta_failed = 0
    ta_corrections = []

    if ta_total == 0:
        print("  Nothing to do.")
    else:
        print(f"  Est. duration : ~{int(ta_total * MB_CALL_SLEEP / 60)} min")
        print()
        try:
            for chunk_start in range(0, ta_total, CHUNK_SIZE):
                chunk = ta_rows[chunk_start: chunk_start + CHUNK_SIZE]
                chunk_num = chunk_start // CHUNK_SIZE + 1
                chunk_total = (ta_total + CHUNK_SIZE - 1) // CHUNK_SIZE
                print(
                    f"  Chunk {chunk_num}/{chunk_total} "
                    f"({chunk_start + 1}-{chunk_start + len(chunk)} of {ta_total})..."
                )
                for (cid, artist, title, old_year) in chunk:
                    year, status = _lookup_title_artist(artist, title)
                    cur.execute("""
                        UPDATE canonical_tracks
                        SET mb_title_artist_year = ?,
                            mb_ta_status         = ?
                        WHERE canonical_id = ?
                    """, (year, status, cid))

                    if status == "SUCCESS":
                        ta_success += 1
                        if year and old_year and year < old_year:
                            ta_corrections.append((artist, title, old_year, year))
                    else:
                        ta_failed += 1

                conn.commit()

        except RuntimeError as e:
            log.error(f"MB title/artist enrichment aborted: {e}")
            conn.close()
            _print_pass_summary(
                "Title/artist", ta_success, ta_failed, 0, ta_corrections, aborted=True
            )
            return _build_result(
                isrc_success, isrc_failed, isrc_no_isrc,
                ta_success, ta_failed, aborted=True
            )

    _print_pass_summary("Title/artist", ta_success, ta_failed, 0, ta_corrections)
    conn.close()
    return _build_result(
        isrc_success, isrc_failed, isrc_no_isrc, ta_success, ta_failed
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _print_pass_summary(label, success, failed, no_isrc, corrections, aborted=False):
    total = success + failed + no_isrc
    print()
    print(f"  --- {label} summary {'(ABORTED)' if aborted else ''} ---")
    print(f"  Looked up : {total}")
    print(f"  SUCCESS   : {success}")
    print(f"  FAILED    : {failed}")
    if no_isrc:
        print(f"  NO_ISRC   : {no_isrc}")
    print(f"  Year improvements (MB < Spotify): {len(corrections)}")
    if corrections:
        corrections.sort(key=lambda x: x[2] - x[3], reverse=True)
        print()
        print(f"  Top improvements (largest shift first):")
        for artist, title, old, new in corrections[:15]:
            print(f"    {artist} - {title}: {old} -> {new}  (shift={old - new}yr)")


def _build_result(isrc_s, isrc_f, isrc_n, ta_s, ta_f, aborted=False):
    return {
        "isrc_success": isrc_s, "isrc_failed": isrc_f, "isrc_no_isrc": isrc_n,
        "ta_success": ta_s, "ta_failed": ta_f,
        "aborted": aborted,
    }


if __name__ == "__main__":
    run_mb_enrichment()
