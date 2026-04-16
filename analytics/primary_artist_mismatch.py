# analytics/primary_artist_mismatch.py
# -*- coding: utf-8 -*-
"""
Primary Artist Mismatch: flag Spotify SUCCESS tracks where display_artist
does not closely match spotify_primary_artist_name.

Background: the enrichment scorer uses max similarity over ALL credited
artists on a Spotify track, so a collab or cover can score 100/100 while
storing a completely different primary artist (Spotify's artists[0]).
Those tracks are invisible to the enrichment_attempt_3_4 report because
their match score looks perfect.  This report surfaces them directly.

Similarity is measured with RapidFuzz token_sort_ratio after normalize_artist()
is applied to both sides.  token_sort_ratio is used (not token_set_ratio) so
that subsets like 'Band Of Gypsys' vs 'The Return Of The Band Of Gypsys' are
not treated as a match.

Output: analytics/outputs/quality_checks/primary_artist_mismatch.csv
Sorted: primary_artist_score ASC (worst first), then play_count DESC.
"""

import sys
import sqlite3
import pandas as pd
from pathlib import Path
from rapidfuzz.fuzz import token_sort_ratio

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scraper.normalization_logic import normalize_artist

DB_PATH = Path(__file__).resolve().parents[1] / "radio_plays.db"
OUTPUT_DIR = Path(__file__).resolve().parent / "outputs"
QUALITY_DIR = OUTPUT_DIR / "quality_checks"
QUALITY_DIR.mkdir(parents=True, exist_ok=True)

# Rows with primary_artist_score below this are written to the report.
# Tune this after reviewing the first real output; 75 is the starting point.
PRIMARY_MISMATCH_THRESHOLD = 75


def _load_candidates():
    """Query all SUCCESS tracks that have a stored primary artist name."""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("""
        SELECT
            canonical_id,
            display_artist,
            display_title,
            spotify_primary_artist_name,
            spotify_artist_score,
            spotify_match_attempt,
            spotify_album_release_year,
            play_count
        FROM canonical_tracks
        WHERE spotify_status = 'SUCCESS'
          AND spotify_primary_artist_name IS NOT NULL
    """, conn)
    conn.close()
    return df


def _compute_scores(df):
    """
    Apply normalize_artist() to both sides and score with token_sort_ratio.
    Rows where either side normalizes to an empty string are dropped
    (nothing meaningful to compare).
    """
    norm_display = df["display_artist"].map(normalize_artist)
    norm_primary = df["spotify_primary_artist_name"].map(normalize_artist)

    scoreable = norm_display.notna() & norm_primary.notna()
    df = df[scoreable].copy()
    norm_display = norm_display[scoreable]
    norm_primary = norm_primary[scoreable]

    df["primary_artist_score"] = [
        int(token_sort_ratio(a, b))
        for a, b in zip(norm_display, norm_primary)
    ]
    return df


def run_primary_artist_mismatch():
    print("=== Primary Artist Mismatch Report ===")
    print()

    print("  Loading SUCCESS tracks...")
    df = _load_candidates()
    print(f"  {len(df)} tracks with stored primary artist")

    print("  Scoring primary artist similarity...")
    df = _compute_scores(df)

    mismatches = df[df["primary_artist_score"] < PRIMARY_MISMATCH_THRESHOLD].copy()
    mismatches = mismatches.sort_values(
        ["primary_artist_score", "play_count"],
        ascending=[True, False],
    )

    col_order = [
        "canonical_id", "display_artist", "display_title",
        "spotify_primary_artist_name",
        "primary_artist_score", "spotify_artist_score",
        "play_count", "spotify_match_attempt", "spotify_album_release_year",
    ]
    mismatches = mismatches[col_order]

    out_path = QUALITY_DIR / "primary_artist_mismatch.csv"
    mismatches.to_csv(out_path, index=False)
    print(
        f"  Saved: {out_path} "
        f"({len(mismatches)} mismatches below threshold={PRIMARY_MISMATCH_THRESHOLD})"
    )
    print()
    print("=== Done ===")


if __name__ == "__main__":
    run_primary_artist_mismatch()
