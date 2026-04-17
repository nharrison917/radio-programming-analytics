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
from pathlib import Path
from urllib.parse import quote

import pandas as pd

from rapidfuzz import fuzz

from scraper.config import DB_PATH, SCRAPER_CONTACT
from scraper.normalization_logic import (
    extract_trailing_parentheticals,
    classify_version_type,
    extract_version_suffix,
)

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

    for attempt in range(3):
        try:
            response = requests.get(
                f"https://musicbrainz.org/ws/2/isrc/{isrc}",
                params={"fmt": "json"},
                headers={"User-Agent": MB_USER_AGENT},
                timeout=30,
            )
            break
        except requests.exceptions.ConnectionError as exc:
            log.warning(f"MB ISRC connection error (attempt {attempt + 1}/3) for {isrc}: {exc}")
            if attempt < 2:
                time.sleep(5 * (attempt + 1))
            else:
                log.warning(f"MB ISRC all retries exhausted for {isrc} -- skipping")
                time.sleep(MB_CALL_SLEEP)
                return None, "FAILED"
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
    # Strip trailing version qualifiers before querying MB.  Titles like
    # "Ride Like The Wind (2019 Remaster)" fail because MB indexes the original
    # title, not the remaster variant.  classify_version_type guards against
    # stripping genuine title parens like "(Don't You) Forget About Me".
    search_title, paren_note = extract_trailing_parentheticals(title)
    if not paren_note or classify_version_type(paren_note) == "other":
        # Parens are not a version note; try dash-style suffix.
        stripped, dash_note = extract_version_suffix(title)
        if dash_note:
            search_title = stripped
        else:
            search_title = title  # nothing meaningful to strip

    # Wrap field values in Lucene quotes so special characters (-,&,(),etc.)
    # in artist/title strings are treated as literals, not query operators.
    safe_artist = quote(artist.replace('"', ''))
    safe_title  = quote(search_title.replace('"', ''))
    url = (
        "https://musicbrainz.org/ws/2/recording"
        f'?query=artist:"{safe_artist}"+AND+recording:"{safe_title}"'
        "&fmt=json&limit=100&inc=releases+release-groups"
    )
    for attempt in range(3):
        try:
            response = requests.get(url, headers={"User-Agent": MB_USER_AGENT}, timeout=30)
            break
        except requests.exceptions.ConnectionError as exc:
            log.warning(
                f"MB title/artist connection error (attempt {attempt + 1}/3) "
                f"for {artist} - {title}: {exc}"
            )
            if attempt < 2:
                time.sleep(5 * (attempt + 1))
            else:
                log.warning(
                    f"MB title/artist all retries exhausted for {artist} - {title} -- skipping"
                )
                time.sleep(MB_CALL_SLEEP)
                return None, "FAILED"
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
# Output helpers
# ---------------------------------------------------------------------------

def _write_mb_failed_csv():
    """Snapshot mb_failed.csv from current DB state.

    Tracks where Spotify succeeded but both MB passes returned FAILED.
    These are the candidates most likely to be playing under a wrong year
    (compilations, remasters, etc.) and worth manual review.
    """
    out_path = Path("analytics/outputs/quality_checks/mb_failed.csv")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("""
        SELECT
            canonical_id, display_artist, display_title, play_count,
            spotify_album_release_year, spotify_album_type, spotify_isrc,
            mb_lookup_status, mb_isrc_year, mb_ta_status, mb_title_artist_year,
            manual_year_override
        FROM canonical_tracks
        WHERE spotify_status = 'SUCCESS'
          AND manual_year_override IS NULL
          AND mb_lookup_status = 'FAILED'
          AND mb_ta_status = 'FAILED'
        ORDER BY display_artist, display_title
    """, conn)
    conn.close()

    df.to_csv(out_path, index=False)
    log.info(f"Wrote {len(df)} rows to {out_path}")


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
          AND manual_year_override IS NULL
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
            _integrity_check(cur)
            conn.close()
            _write_mb_failed_csv()
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
          AND manual_year_override IS NULL
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
                            mb_ta_status         = ?,
                            mb_looked_up_at      = DATETIME('now')
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
            _integrity_check(cur)
            conn.close()
            _write_mb_failed_csv()
            _print_pass_summary(
                "Title/artist", ta_success, ta_failed, 0, ta_corrections, aborted=True
            )
            return _build_result(
                isrc_success, isrc_failed, isrc_no_isrc,
                ta_success, ta_failed, aborted=True
            )

    _print_pass_summary("Title/artist", ta_success, ta_failed, 0, ta_corrections)
    _integrity_check(cur)
    conn.close()
    _write_mb_failed_csv()
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


def _integrity_check(cur):
    """Check for rows where status is SUCCESS but year data is NULL.

    These would indicate a partial write (status committed without data).
    Logs a warning for each check that finds anomalies; otherwise silent.
    """
    checks = [
        (
            "mb_lookup_status = 'SUCCESS' AND mb_isrc_year IS NULL",
            "mb_isrc_year",
            "mb_lookup_status",
        ),
        (
            "mb_ta_status = 'SUCCESS' AND mb_title_artist_year IS NULL",
            "mb_title_artist_year",
            "mb_ta_status",
        ),
    ]
    found_any = False
    for where, year_col, status_col in checks:
        cur.execute(
            f"SELECT COUNT(*) FROM canonical_tracks WHERE {where}"
        )
        count = cur.fetchone()[0]
        if count > 0:
            log.warning(
                f"INTEGRITY: {count} row(s) have {status_col}='SUCCESS' "
                f"but {year_col} IS NULL -- possible partial write"
            )
            found_any = True
    if not found_any:
        log.info("INTEGRITY: MB year columns consistent (no SUCCESS rows with NULL year)")


def _build_result(isrc_s, isrc_f, isrc_n, ta_s, ta_f, aborted=False):
    return {
        "isrc_success": isrc_s, "isrc_failed": isrc_f, "isrc_no_isrc": isrc_n,
        "ta_success": ta_s, "ta_failed": ta_f,
        "aborted": aborted,
    }


if __name__ == "__main__":
    run_mb_enrichment()
