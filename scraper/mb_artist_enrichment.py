# -*- coding: utf-8 -*-
"""
MusicBrainz artist enrichment for canonical_artists.

Populates mb_artist_id and mb_earliest_release_year so that
band_age_at_recording (best_year - mb_earliest_release_year) can be
computed per track for show-level analytics.

Two sequential passes:

  Pass A -- Artist MBID resolution
    For each artist without mb_artist_id:
      1. Take one ISRC from a linked SUCCESS track and call the MB ISRC
         endpoint -- the recording response includes artist-credit with MB
         artist MBIDs, so no extra call is needed.
      2. Fallback for artists with no usable ISRC: MB artist name search
         with fuzzy match.
    The MBID is written to canonical_artists.mb_artist_id.

  Pass B -- Release-group browse
    For each artist with mb_artist_id set:
      Browse all release-groups (type=album|single|ep) and find the
      minimum first-release-date year.  Compilations are excluded on our
      side even if they slip through the type filter.
    Results written to mb_earliest_release_year + mb_artist_status.

Rate limit: 1 request/second (MusicBrainz requirement).
"""

import sqlite3
import requests
import time
import logging
from collections import Counter
from datetime import datetime
from pathlib import Path

import pandas as pd
from rapidfuzz import fuzz

from scraper.config import DB_PATH, SCRAPER_CONTACT

QUALITY_DIR = Path("analytics/outputs/quality_checks")

MB_CALL_SLEEP = 1.1          # seconds between every API call
CHUNK_SIZE = 50              # artists per progress-reporting chunk
RETRY_AFTER_DAYS = 7        # days before retrying a FAILED artist
NAME_FUZZY_THRESHOLD = 90   # minimum token_set_ratio for name-search fallback
ISRC_REJECT_THRESHOLD = 55  # if ISRC-derived artist name scores below this, reject
                             # and fall back to name search (catches wrong primary
                             # artist credit, e.g. "Gary Jules ft." ISRC credited
                             # to producer).  Note: token_set_ratio is still prone
                             # to false-positives for short names that are substrings
                             # of longer names ("Dada" <= "Dada Life"); those cases
                             # require manual correction via set-artist-meta.
YEAR_DELTA_LOG = 5          # log when |mb_earliest - spotify_earliest| > this

MB_USER_AGENT = f"radio-scraper/1.0 ({SCRAPER_CONTACT})"

log = logging.getLogger(__name__)

CURRENT_YEAR = datetime.utcnow().year


# ---------------------------------------------------------------------------
# Shared HTTP helper
# ---------------------------------------------------------------------------

def _get(url, params=None):
    """GET with retry on ConnectionError.  Returns Response or raises RuntimeError."""
    for attempt in range(3):
        try:
            resp = requests.get(
                url,
                params=params,
                headers={"User-Agent": MB_USER_AGENT},
                timeout=30,
            )
            return resp
        except requests.exceptions.ConnectionError as exc:
            log.warning(f"MB connection error (attempt {attempt + 1}/3): {exc}")
            if attempt < 2:
                time.sleep(5 * (attempt + 1))
    raise RuntimeError(f"MB connection failed after 3 attempts: {url}")


# ---------------------------------------------------------------------------
# Pass A helpers: artist MBID resolution
# ---------------------------------------------------------------------------

def _mbid_from_isrc_response(data, expected_artist):
    """Extract the most-common primary artist MBID from an ISRC response.

    MB can map one ISRC to multiple recordings (re-releases, remasters).
    We take the mode of all first-credit artist MBIDs.  If the result
    does not fuzzy-match expected_artist at >= NAME_FUZZY_THRESHOLD we
    log a warning but still return it -- the ISRC link is the stronger
    signal.

    Returns MBID string or None.
    """
    recordings = data.get("recordings", [])
    mbids = []
    names = []
    for rec in recordings:
        credits = rec.get("artist-credit", [])
        if not credits or not isinstance(credits[0], dict):
            continue
        artist_obj = credits[0].get("artist", {})
        mbid = artist_obj.get("id")
        name = artist_obj.get("name", "")
        if mbid:
            mbids.append(mbid)
            names.append(name)

    if not mbids:
        return None

    mbid = Counter(mbids).most_common(1)[0][0]

    # Spot-check name against what we expect
    matched_name = names[mbids.index(mbid)] if mbid in mbids else ""
    score = fuzz.token_set_ratio(expected_artist.lower(), matched_name.lower())
    if score < ISRC_REJECT_THRESHOLD:
        log.warning(
            f"MBID rejected (score={score:.0f} < {ISRC_REJECT_THRESHOLD}): "
            f"expected '{expected_artist}', got '{matched_name}' "
            f"-- discarding ISRC-derived MBID, falling back to name search"
        )
        return None

    if score < NAME_FUZZY_THRESHOLD:
        log.warning(
            f"MBID name mismatch: expected '{expected_artist}', "
            f"got '{matched_name}' (score={score:.0f}) -- keeping MBID {mbid}"
        )

    return mbid


def _resolve_via_isrc(isrc, expected_artist):
    """Call MB ISRC endpoint and extract primary artist MBID.

    Returns (mbid, method) where method is 'isrc' or None on failure.
    Always sleeps MB_CALL_SLEEP.
    """
    resp = _get(f"https://musicbrainz.org/ws/2/isrc/{isrc.upper()}",
                params={"fmt": "json", "inc": "artist-credits"})
    time.sleep(MB_CALL_SLEEP)

    if resp.status_code == 404:
        return None, None
    if resp.status_code == 503:
        log.warning(f"MB 503 on ISRC {isrc}")
        return None, None
    if resp.status_code != 200:
        raise RuntimeError(f"MB unexpected {resp.status_code} for ISRC {isrc}")

    mbid = _mbid_from_isrc_response(resp.json(), expected_artist)
    return (mbid, "isrc") if mbid else (None, None)


def _resolve_via_name(artist_name):
    """Search MB artist by name, return best-match MBID.

    Uses MB score (0-100) as primary signal, then verifies with fuzzy
    name match.  Only accepts results where both MB score >= 85 and
    fuzzy >= NAME_FUZZY_THRESHOLD.

    Returns (mbid, method) where method is 'name' or None on failure.
    Always sleeps MB_CALL_SLEEP.
    """
    safe_name = artist_name.replace('"', '')
    resp = _get(
        "https://musicbrainz.org/ws/2/artist",
        params={"query": f'artist:"{safe_name}"', "fmt": "json", "limit": 5},
    )
    time.sleep(MB_CALL_SLEEP)

    if resp.status_code == 503:
        log.warning(f"MB 503 on name search for '{artist_name}'")
        return None, None
    if resp.status_code != 200:
        raise RuntimeError(f"MB unexpected {resp.status_code} for name search '{artist_name}'")

    artists = resp.json().get("artists", [])
    for candidate in artists:
        mb_score = int(candidate.get("score", 0))
        if mb_score < 85:
            break  # results are score-sorted; no point checking further
        mb_name = candidate.get("name", "")
        fuzzy_score = fuzz.token_set_ratio(artist_name.lower(), mb_name.lower())
        if fuzzy_score >= NAME_FUZZY_THRESHOLD:
            return candidate["id"], "name"

    return None, None


def _run_pass_a(conn):
    """Resolve MB artist MBIDs for all artists without one.

    Strategy per artist:
      1. Pick one spotify_isrc from a linked SUCCESS canonical track.
      2. Call MB ISRC endpoint; extract artist MBID from recording credits.
      3. If no ISRC available or ISRC not in MB: fall back to name search.
    """
    cur = conn.cursor()

    # Fetch all artists needing resolution, with one candidate ISRC each
    cur.execute("""
        SELECT
            ca.spotify_artist_id,
            ca.artist_name,
            ca.earliest_release_year AS spotify_earliest,
            (
                SELECT ct.spotify_isrc
                FROM canonical_tracks ct
                WHERE ct.spotify_primary_artist_id = ca.spotify_artist_id
                  AND ct.spotify_isrc IS NOT NULL
                  AND ct.spotify_status = 'SUCCESS'
                LIMIT 1
            ) AS sample_isrc
        FROM canonical_artists ca
        WHERE ca.mb_artist_id IS NULL
          AND (ca.mb_artist_status IS NULL OR ca.mb_artist_status != 'NO_MATCH')
        ORDER BY ca.artist_name
    """)
    rows = cur.fetchall()
    total = len(rows)

    print("=== Pass A: Artist MBID resolution ===")
    print(f"  Artists needing MBID : {total}")
    if total == 0:
        print("  Nothing to do.")
        print()
        return

    isrc_success = 0
    name_success = 0
    failed = 0

    for i, (artist_id, artist_name, spotify_earliest, sample_isrc) in enumerate(rows, 1):
        if i % CHUNK_SIZE == 1:
            chunk_num = (i - 1) // CHUNK_SIZE + 1
            chunk_total = (total + CHUNK_SIZE - 1) // CHUNK_SIZE
            print(f"  Chunk {chunk_num}/{chunk_total}  ({i}-{min(i + CHUNK_SIZE - 1, total)} of {total})...")

        mbid = None
        method = None

        # Step 1: Try ISRC
        if sample_isrc:
            try:
                mbid, method = _resolve_via_isrc(sample_isrc, artist_name)
            except RuntimeError as e:
                log.error(f"Pass A ISRC aborted: {e}")
                conn.commit()
                _print_pass_a_summary(isrc_success, name_success, failed, total)
                raise

        # Step 2: Name fallback
        if mbid is None:
            try:
                mbid, method = _resolve_via_name(artist_name)
            except RuntimeError as e:
                log.error(f"Pass A name search aborted: {e}")
                conn.commit()
                _print_pass_a_summary(isrc_success, name_success, failed, total)
                raise

        if mbid:
            cur.execute(
                "UPDATE canonical_artists SET mb_artist_id = ? WHERE spotify_artist_id = ?",
                (mbid, artist_id),
            )
            if method == "isrc":
                isrc_success += 1
            else:
                name_success += 1
            log.debug(f"  [{method}] {artist_name} -> {mbid}")
        else:
            failed += 1
            log.info(f"  [no match] {artist_name}")

        # Commit every chunk
        if i % CHUNK_SIZE == 0:
            conn.commit()

    conn.commit()
    _print_pass_a_summary(isrc_success, name_success, failed, total)


def _print_pass_a_summary(isrc_ok, name_ok, failed, total):
    print()
    print("  --- Pass A summary ---")
    print(f"  Processed  : {isrc_ok + name_ok + failed} / {total}")
    print(f"  Via ISRC   : {isrc_ok}")
    print(f"  Via name   : {name_ok}")
    print(f"  No match   : {failed}")
    print()


# ---------------------------------------------------------------------------
# Pass B helpers: release-group browse
# ---------------------------------------------------------------------------

VALID_PRIMARY_TYPES = {"Album", "Single", "EP"}


def _earliest_rg_year(mbid):
    """Browse all Album/Single/EP release-groups for an MB artist.

    Paginates until all release-groups are fetched.  Compilations are
    excluded even if they slipped through MB's type filter.

    Returns earliest valid year (int) or None.
    """
    offset = 0
    limit = 100
    earliest = None
    total_rgs = None  # filled on first response

    while True:
        resp = _get(
            "https://musicbrainz.org/ws/2/release-group",
            params={
                "artist": mbid,
                "type": "album|single|ep",
                "limit": limit,
                "offset": offset,
                "fmt": "json",
            },
        )
        time.sleep(MB_CALL_SLEEP)

        if resp.status_code == 404:
            return None
        if resp.status_code == 503:
            log.warning(f"MB 503 on release-group browse for {mbid}")
            return None
        if resp.status_code != 200:
            raise RuntimeError(
                f"MB unexpected {resp.status_code} for release-group browse {mbid}"
            )

        data = resp.json()
        rgs = data.get("release-groups", [])
        if total_rgs is None:
            total_rgs = data.get("release-group-count", 0)

        for rg in rgs:
            primary = rg.get("primary-type", "")
            secondary_raw = rg.get("secondary-types", [])
            secondary = [
                s if isinstance(s, str) else s.get("name", "")
                for s in secondary_raw
            ]

            if primary not in VALID_PRIMARY_TYPES:
                continue
            if "Compilation" in secondary or "Live" in secondary:
                continue

            frd = rg.get("first-release-date", "")
            if not frd or len(str(frd)) < 4:
                continue
            try:
                year = int(str(frd)[:4])
            except (ValueError, TypeError):
                continue
            if 1920 <= year <= CURRENT_YEAR + 1:
                if earliest is None or year < earliest:
                    earliest = year

        offset += len(rgs)
        if not rgs or offset >= (total_rgs or 0):
            break

    return earliest


def _run_pass_b(conn):
    """Browse release-groups and populate mb_earliest_release_year."""
    cur = conn.cursor()

    cur.execute(f"""
        SELECT
            spotify_artist_id,
            artist_name,
            mb_artist_id,
            earliest_release_year AS spotify_earliest
        FROM canonical_artists
        WHERE mb_artist_id IS NOT NULL
          AND (
              mb_artist_status IS NULL
              OR (
                  mb_artist_status = 'FAILED'
                  AND mb_artist_last_attempted_at < DATETIME('now', '-{RETRY_AFTER_DAYS} days')
              )
          )
        ORDER BY artist_name
    """)
    rows = cur.fetchall()
    total = len(rows)

    print("=== Pass B: Release-group browse ===")
    print(f"  Artists eligible : {total}")
    if total == 0:
        print("  Nothing to do.")
        print()
        return

    success = 0
    failed = 0
    large_deltas = []   # (artist_name, mb_year, spotify_year, delta)

    for i, (artist_id, artist_name, mbid, spotify_earliest) in enumerate(rows, 1):
        if i % CHUNK_SIZE == 1:
            chunk_num = (i - 1) // CHUNK_SIZE + 1
            chunk_total = (total + CHUNK_SIZE - 1) // CHUNK_SIZE
            print(f"  Chunk {chunk_num}/{chunk_total}  ({i}-{min(i + CHUNK_SIZE - 1, total)} of {total})...")

        try:
            year = _earliest_rg_year(mbid)
        except RuntimeError as e:
            log.error(f"Pass B aborted: {e}")
            conn.commit()
            _print_pass_b_summary(success, failed, total, large_deltas)
            raise

        if year is not None:
            status = "SUCCESS"
            success += 1

            # Log notable divergence from Spotify's earliest_release_year
            if spotify_earliest is not None:
                delta = abs(year - spotify_earliest)
                if delta > YEAR_DELTA_LOG:
                    large_deltas.append((artist_name, year, spotify_earliest, delta))
                    log.info(
                        f"YEAR_DELTA: {artist_name}: MB={year}, Spotify={spotify_earliest} "
                        f"(delta={delta}yr)"
                    )
        else:
            status = "FAILED"
            failed += 1
            year = None

        cur.execute("""
            UPDATE canonical_artists
            SET mb_earliest_release_year  = ?,
                mb_artist_status          = ?,
                mb_artist_last_attempted_at = DATETIME('now')
            WHERE spotify_artist_id = ?
        """, (year, status, artist_id))

        if i % CHUNK_SIZE == 0:
            conn.commit()

    conn.commit()
    _print_pass_b_summary(success, failed, total, large_deltas)


def _print_pass_b_summary(success, failed, total, large_deltas):
    print()
    print("  --- Pass B summary ---")
    print(f"  Processed  : {success + failed} / {total}")
    print(f"  SUCCESS    : {success}")
    print(f"  FAILED     : {failed}")
    print(f"  Large deltas (|MB - Spotify| > {YEAR_DELTA_LOG}yr): {len(large_deltas)}")
    if large_deltas:
        large_deltas.sort(key=lambda x: x[3], reverse=True)
        print()
        print(f"  Top divergences (MB vs Spotify earliest):")
        for name, mb_yr, sp_yr, delta in large_deltas[:20]:
            print(f"    {name:<40} MB={mb_yr}  Spotify={sp_yr}  delta={delta}yr")
    print()


# ---------------------------------------------------------------------------
# Quality report writers
# ---------------------------------------------------------------------------

def _write_mb_artist_missing_csv():
    """Snapshot of artists still awaiting MB resolution (excluding NO_MATCH).

    Columns: artist_name, mb_artist_status, spotify_earliest_year,
             track_count, play_count, sample_track, has_isrc
    """
    QUALITY_DIR.mkdir(parents=True, exist_ok=True)
    out_path = QUALITY_DIR / "mb_artist_missing.csv"

    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("""
        SELECT
            ca.artist_name,
            COALESCE(ca.mb_artist_status, 'NULL') AS mb_artist_status,
            ca.earliest_release_year              AS spotify_earliest_year,
            COUNT(DISTINCT ct.canonical_id)        AS track_count,
            SUM(ct.play_count)                     AS play_count,
            MIN(ct.display_title)                  AS sample_track,
            CASE WHEN EXISTS (
                SELECT 1 FROM canonical_tracks ct2
                WHERE ct2.spotify_primary_artist_id = ca.spotify_artist_id
                  AND ct2.spotify_isrc IS NOT NULL
                  AND ct2.spotify_status = 'SUCCESS'
            ) THEN 1 ELSE 0 END                    AS has_isrc
        FROM canonical_artists ca
        LEFT JOIN canonical_tracks ct
               ON ct.spotify_primary_artist_id = ca.spotify_artist_id
        WHERE ca.mb_artist_id IS NULL
          AND (ca.mb_artist_status IS NULL OR ca.mb_artist_status = 'FAILED')
        GROUP BY ca.spotify_artist_id
        ORDER BY play_count DESC, ca.artist_name
    """, conn)
    conn.close()

    df.to_csv(out_path, index=False)
    print(f"  Wrote {len(df)} rows to {out_path}")
    log.info(f"Wrote {len(df)} rows to {out_path}")


def _write_mb_artist_large_delta_csv():
    """Artists where |mb_earliest_release_year - spotify_earliest_year| > YEAR_DELTA_LOG.

    Sorted by abs_delta descending.  mb_later_than_spotify=1 flags cases where
    MB found a later start than Spotify -- these are the most likely to indicate
    a wrong MBID or bad MB date and warrant manual review.
    """
    QUALITY_DIR.mkdir(parents=True, exist_ok=True)
    out_path = QUALITY_DIR / "mb_artist_large_delta.csv"

    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(f"""
        SELECT
            ca.artist_name,
            ca.earliest_release_year                                           AS spotify_earliest_year,
            ca.mb_earliest_release_year                                        AS mb_earliest_year,
            ca.mb_earliest_release_year - ca.earliest_release_year            AS delta,
            ABS(ca.mb_earliest_release_year - ca.earliest_release_year)       AS abs_delta,
            CASE WHEN ca.mb_earliest_release_year > ca.earliest_release_year
                 THEN 1 ELSE 0 END                                             AS mb_later_than_spotify,
            ca.mb_artist_id,
            SUM(ct.play_count)                                                 AS play_count
        FROM canonical_artists ca
        LEFT JOIN canonical_tracks ct
               ON ct.spotify_primary_artist_id = ca.spotify_artist_id
        WHERE ca.mb_artist_status = 'SUCCESS'
          AND ca.earliest_release_year IS NOT NULL
          AND ca.mb_earliest_release_year IS NOT NULL
          AND ABS(ca.mb_earliest_release_year - ca.earliest_release_year) > {YEAR_DELTA_LOG}
        GROUP BY ca.spotify_artist_id
        ORDER BY abs_delta DESC, ca.artist_name
    """, conn)
    conn.close()

    df.to_csv(out_path, index=False)
    print(f"  Wrote {len(df)} rows to {out_path}")
    log.info(f"Wrote {len(df)} rows to {out_path}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run_mb_artist_enrichment():
    """Run both passes.  Each is independently idempotent."""
    conn = sqlite3.connect(DB_PATH)

    print()
    print("=== MusicBrainz Artist Enrichment ===")
    print()

    try:
        _run_pass_a(conn)
        _run_pass_b(conn)
    except RuntimeError:
        # Already logged; return so caller can handle
        conn.close()
        return

    # Integrity check: SUCCESS rows should always have a year
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM canonical_artists
        WHERE mb_artist_status = 'SUCCESS' AND mb_earliest_release_year IS NULL
    """)
    bad = cur.fetchone()[0]
    if bad:
        log.warning(f"INTEGRITY: {bad} rows have mb_artist_status='SUCCESS' but NULL year")
    else:
        log.info("INTEGRITY: mb_artist columns consistent")

    conn.close()

    print("  Writing quality reports...")
    _write_mb_artist_missing_csv()
    _write_mb_artist_large_delta_csv()

    print("=== Done ===")


# ---------------------------------------------------------------------------
# Manual MBID correction
# ---------------------------------------------------------------------------

def run_set_artist_meta(artist_name, mb_id):
    """Correct a wrong mb_artist_id and immediately re-run Pass B for that artist.

    Looks up the artist by exact name (case-insensitive).  Updates mb_artist_id,
    clears the existing Pass B results, then calls the release-group browse
    inline so the result is visible immediately.

    Usage:
        python rs_main.py set-artist-meta --artist-name "Dada" --mb-id "UUID"
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        SELECT spotify_artist_id, artist_name, mb_artist_id, mb_earliest_release_year
        FROM canonical_artists
        WHERE LOWER(artist_name) = LOWER(?)
    """, (artist_name,))
    row = cur.fetchone()

    if not row:
        print(f"Artist not found: '{artist_name}'")
        print("Check spelling -- name must match canonical_artists.artist_name exactly (case-insensitive).")
        conn.close()
        return

    spotify_artist_id, found_name, old_mbid, old_year = row
    print(f"Artist  : {found_name}")
    print(f"Old MBID: {old_mbid}  (year was {old_year})")
    print(f"New MBID: {mb_id}")
    print()

    cur.execute("""
        UPDATE canonical_artists
        SET mb_artist_id               = ?,
            mb_earliest_release_year   = NULL,
            mb_artist_status           = NULL,
            mb_artist_last_attempted_at = NULL
        WHERE spotify_artist_id = ?
    """, (mb_id, spotify_artist_id))
    conn.commit()

    print("Running Pass B for this artist...")
    year = _earliest_rg_year(mb_id)
    status = "SUCCESS" if year is not None else "FAILED"

    cur.execute("""
        UPDATE canonical_artists
        SET mb_earliest_release_year   = ?,
            mb_artist_status           = ?,
            mb_artist_last_attempted_at = DATETIME('now')
        WHERE spotify_artist_id = ?
    """, (year, status, spotify_artist_id))
    conn.commit()
    conn.close()

    if year is not None:
        print(f"mb_earliest_release_year = {year}  (status={status})")
    else:
        print(f"Pass B returned no year -- status=FAILED. Check the MBID.")


if __name__ == "__main__":
    run_mb_artist_enrichment()
