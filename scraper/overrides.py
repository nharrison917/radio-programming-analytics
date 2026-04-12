# -*- coding: utf-8 -*-
"""
Manual override helpers for canonical_tracks.

Two commands:

  add-override  --id <canonical_id> --spotify-id <spotify_id>
      Inserts a row into manual_spotify_overrides. The next 'weekly' run will
      enrich the track via Spotify (year, duration, ISRC, artist ID, etc).

  set-meta  --id <canonical_id> [--year YYYY|YYYY-MM-DD] [--duration M:SS]
      Writes manual_year_override + manual_release_date and/or
      manual_duration_ms directly to canonical_tracks. For tracks that are
      genuinely not on Spotify.
"""

import re
import sqlite3

from scraper.config import DB_PATH
from scraper.db import migrate_db


# ---------------------------------------------------------------------------
# Input parsers
# ---------------------------------------------------------------------------

def _parse_year_input(raw):
    """Accept YYYY or YYYY-MM-DD. Returns (year_int, date_str)."""
    raw = raw.strip()
    if re.fullmatch(r"\d{4}", raw):
        return int(raw), raw
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw):
        year = int(raw[:4])
        if year < 1920 or year > 2100:
            raise ValueError(f"Year {year} out of plausible range (1920-2100)")
        return year, raw
    raise ValueError(f"Year must be YYYY or YYYY-MM-DD, got: {raw!r}")


def _parse_duration_input(raw):
    """Accept M:SS or MM:SS. Returns duration_ms as int."""
    raw = raw.strip()
    m = re.fullmatch(r"(\d{1,2}):(\d{2})", raw)
    if not m:
        raise ValueError(f"Duration must be M:SS or MM:SS, got: {raw!r}")
    minutes = int(m.group(1))
    seconds = int(m.group(2))
    if seconds >= 60:
        raise ValueError(f"Seconds must be 00-59, got: {seconds:02d}")
    return (minutes * 60 + seconds) * 1000


def _format_duration(ms):
    """Format duration_ms as M:SS for display. Returns '(none)' for None."""
    if ms is None:
        return "(none)"
    total_s = ms // 1000
    return f"{total_s // 60}:{total_s % 60:02d}"


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _fetch_canonical(cur, canonical_id):
    cur.execute("""
        SELECT canonical_id, display_artist, display_title,
               spotify_status, spotify_album_release_year,
               manual_year_override, manual_release_date, manual_duration_ms
        FROM canonical_tracks
        WHERE canonical_id = ?
    """, (canonical_id,))
    return cur.fetchone()


def _fetch_existing_override(cur, canonical_id):
    cur.execute(
        "SELECT spotify_id FROM manual_spotify_overrides WHERE canonical_id = ?",
        (canonical_id,)
    )
    row = cur.fetchone()
    return row[0] if row else None


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def run_add_override(canonical_id, spotify_id):
    """Insert or replace a manual Spotify ID override for a FAILED canonical."""
    migrate_db()

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    row = _fetch_canonical(cur, canonical_id)
    if row is None:
        print(f"No canonical found with id={canonical_id}")
        conn.close()
        return

    cid, artist, title, status, sp_year, yr_override, rel_date, dur_ms = row
    existing_id = _fetch_existing_override(cur, cid)

    print()
    print(f"canonical {cid} | {artist} - {title}")
    print(f"  spotify_status   : {status}")
    print(f"  current override : {existing_id if existing_id else '(none)'}")
    print()
    print(f"Setting: spotify_id={spotify_id}")

    answer = input("Proceed? [y/N]: ").strip().lower()
    if answer != "y":
        print("Aborted.")
        conn.close()
        return

    cur.execute(
        "INSERT OR REPLACE INTO manual_spotify_overrides (canonical_id, spotify_id) VALUES (?, ?)",
        (cid, spotify_id)
    )
    conn.commit()
    conn.close()
    print(f"Override saved. Run 'python rs_main.py weekly' to enrich.")


def run_set_meta(canonical_id, year_raw=None, duration_raw=None):
    """Write manual year and/or duration directly to canonical_tracks."""
    if year_raw is None and duration_raw is None:
        print("set-meta requires at least --year or --duration.")
        return

    year_int = None
    date_str = None
    duration_ms = None

    if year_raw is not None:
        try:
            year_int, date_str = _parse_year_input(year_raw)
        except ValueError as e:
            print(f"Invalid --year: {e}")
            return

    if duration_raw is not None:
        try:
            duration_ms = _parse_duration_input(duration_raw)
        except ValueError as e:
            print(f"Invalid --duration: {e}")
            return

    migrate_db()

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    row = _fetch_canonical(cur, canonical_id)
    if row is None:
        print(f"No canonical found with id={canonical_id}")
        conn.close()
        return

    cid, artist, title, status, sp_year, yr_override, rel_date, dur_ms_current = row

    # Setting a year on a FAILED track is an authoritative closure — mark it
    # NO_MATCH so the enrichment pipeline stops retrying and the failures CSV
    # excludes it.  Leave SUCCESS/PENDING/NON_MUSIC/NO_MATCH statuses alone.
    will_close = year_int is not None and status == "FAILED"

    print()
    print(f"canonical {cid} | {artist} - {title}")
    print(f"  spotify_status             : {status}{' -> NO_MATCH' if will_close else ''}")
    print(f"  spotify_album_release_year : {sp_year if sp_year is not None else '(none)'}")
    print(f"  manual_year_override       : {yr_override if yr_override is not None else '(none)'}")
    print(f"  manual_release_date        : {rel_date if rel_date else '(none)'}")
    print(f"  manual_duration_ms         : {_format_duration(dur_ms_current)}")
    print()

    changes = []
    if year_int is not None:
        changes.append(f"year={year_int}, release_date={date_str!r}")
    if duration_ms is not None:
        changes.append(f"duration={_format_duration(duration_ms)} ({duration_ms} ms)")
    if will_close:
        changes.append("spotify_status=NO_MATCH (stops Spotify retries)")
    print(f"Setting: {', '.join(changes)}")

    answer = input("Proceed? [y/N]: ").strip().lower()
    if answer != "y":
        print("Aborted.")
        conn.close()
        return

    if year_int is not None:
        cur.execute("""
            UPDATE canonical_tracks
            SET manual_year_override = ?, manual_release_date = ?
            WHERE canonical_id = ?
        """, (year_int, date_str, cid))

    if will_close:
        cur.execute("""
            UPDATE canonical_tracks
            SET spotify_status = 'NO_MATCH'
            WHERE canonical_id = ?
        """, (cid,))

    if duration_ms is not None:
        cur.execute("""
            UPDATE canonical_tracks
            SET manual_duration_ms = ?
            WHERE canonical_id = ?
        """, (duration_ms, cid))

    conn.commit()
    conn.close()
    print("Saved.")
