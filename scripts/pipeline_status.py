# -*- coding: utf-8 -*-
"""
pipeline_status.py

Prints a at-a-glance summary of pipeline backfill progress.
Safe to run anytime -- read-only DB queries.
"""

import sqlite3
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "radio_plays.db"

REMASTER_SIGNALS = ["remaster", "deluxe", "anniversary", "expanded", "edition"]


def _pct(n, total):
    if total == 0:
        return 0.0
    return 100.0 * n / total


def _bar(n, total, width=20):
    if total == 0:
        return "[" + "-" * width + "]"
    filled = int(round(width * n / total))
    filled = min(filled, width)
    return "[" + "#" * filled + "-" * (width - filled) + "]"


def run_status():
    if not DB_PATH.exists():
        print("ERROR: radio_plays.db not found at", DB_PATH)
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # --- Dataset basics ---
    cur.execute("SELECT COUNT(*) FROM plays")
    total_plays = cur.fetchone()[0]

    cur.execute("SELECT MIN(play_ts), MAX(play_ts) FROM plays")
    min_ts, max_ts = cur.fetchone()
    min_date = min_ts[:10] if min_ts else "?"
    max_date = max_ts[:10] if max_ts else "?"

    cur.execute("SELECT COUNT(DISTINCT DATE(play_ts)) FROM plays")
    distinct_days = cur.fetchone()[0]

    # --- Canonical tracks ---
    cur.execute("SELECT COUNT(*) FROM canonical_tracks")
    total_tracks = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM canonical_tracks WHERE spotify_status = 'SUCCESS'")
    success_tracks = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM canonical_tracks WHERE spotify_status = 'FAILED'")
    failed_tracks = cur.fetchone()[0]

    # --- ISRC backfill ---
    cur.execute(
        "SELECT COUNT(*) FROM canonical_tracks "
        "WHERE spotify_status = 'SUCCESS' AND spotify_isrc IS NOT NULL"
    )
    has_isrc = cur.fetchone()[0]

    cur.execute(
        "SELECT COUNT(*) FROM canonical_tracks "
        "WHERE spotify_status = 'SUCCESS' AND spotify_isrc IS NULL"
    )
    no_isrc = cur.fetchone()[0]

    # --- MB lookup ---
    remaster_clauses = " OR ".join(
        f"LOWER(COALESCE(spotify_album_name,'')) LIKE '%{s}%'"
        for s in REMASTER_SIGNALS
    )
    mb_filter = f"(spotify_album_type = 'compilation' OR {remaster_clauses})"

    cur.execute(
        f"SELECT COUNT(*) FROM canonical_tracks "
        f"WHERE spotify_status = 'SUCCESS' AND spotify_isrc IS NOT NULL AND {mb_filter}"
    )
    mb_eligible = cur.fetchone()[0]

    cur.execute(
        "SELECT COUNT(*) FROM canonical_tracks WHERE mb_lookup_status = 'SUCCESS'"
    )
    mb_success = cur.fetchone()[0]

    cur.execute(
        "SELECT COUNT(*) FROM canonical_tracks WHERE mb_lookup_status = 'FAILED'"
    )
    mb_failed = cur.fetchone()[0]

    # Tracks that will become MB-eligible once ISRC backfill reaches them
    cur.execute(
        f"SELECT COUNT(*) FROM canonical_tracks "
        f"WHERE spotify_status = 'SUCCESS' AND spotify_isrc IS NULL AND {mb_filter}"
    )
    mb_pending_isrc = cur.fetchone()[0]

    cur.execute(
        "SELECT COUNT(*) FROM canonical_tracks "
        "WHERE (mb_isrc_year IS NOT NULL OR mb_title_artist_year IS NOT NULL) "
        "  AND spotify_album_release_year IS NOT NULL "
        "  AND (   (mb_isrc_year IS NOT NULL AND mb_isrc_year < spotify_album_release_year) "
        "       OR (mb_title_artist_year IS NOT NULL AND mb_title_artist_year < spotify_album_release_year))"
    )
    mb_improved = cur.fetchone()[0]

    # --- Artist enrichment ---
    cur.execute("SELECT COUNT(*) FROM canonical_artists")
    total_artists = cur.fetchone()[0]

    cur.execute(
        "SELECT COUNT(*) FROM canonical_artists WHERE enrichment_status = 'SUCCESS'"
    )
    artists_done = cur.fetchone()[0]

    cur.execute(
        "SELECT COUNT(*) FROM canonical_artists WHERE enrichment_status = 'PENDING'"
    )
    artists_pending = cur.fetchone()[0]

    conn.close()

    # --- Print ---
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    print()
    print(f"=== Pipeline Status  {now} ===")
    print()
    print(f"  Dataset:  {total_plays:,} plays  |  {distinct_days} days  |  {min_date} to {max_date}")
    print(f"  Tracks:   {success_tracks:,} SUCCESS  |  {failed_tracks} FAILED  |  {total_tracks:,} total")
    print()
    print("  --- Backfill progress ---")
    print()

    # ISRC backfill
    print(
        f"  ISRC backfill    {_bar(has_isrc, success_tracks)}  "
        f"{has_isrc:,} / {success_tracks:,}  ({_pct(has_isrc, success_tracks):.1f}%)"
        + (f"  -- {no_isrc} remaining" if no_isrc else "  -- COMPLETE")
    )

    # MB lookups
    mb_done_str = f"{mb_success} / {mb_eligible} eligible"
    mb_extra = []
    if mb_failed:
        mb_extra.append(f"{mb_failed} FAILED")
    if mb_pending_isrc:
        mb_extra.append(f"~{mb_pending_isrc} more once ISRC backfill done")
    mb_note = ("  -- " + ", ".join(mb_extra)) if mb_extra else "  -- caught up"
    print(
        f"  MB lookups       {_bar(mb_success, mb_eligible)}  "
        f"{mb_done_str}  ({_pct(mb_success, mb_eligible):.1f}%)"
        + mb_note
    )
    print(f"                   {mb_improved} year corrections applied so far")

    # Artist enrichment
    print(
        f"  Artist enrich    {_bar(artists_done, total_artists)}  "
        f"{artists_done:,} / {total_artists:,}  ({_pct(artists_done, total_artists):.1f}%)"
        + (f"  -- {artists_pending} remaining" if artists_pending else "  -- COMPLETE")
    )

    print()
    if failed_tracks:
        print(f"  ACTION: {failed_tracks} FAILED tracks -- see analytics/outputs/enrichment_failures.csv")
    print()


if __name__ == "__main__":
    run_status()
