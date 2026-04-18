# -*- coding: utf-8 -*-
"""
Pre-Release Plays Report

Identifies plays that occurred before the track's Spotify release date.
One row per (week, canonical_id): the most days-before-release play in that
week is used so back-to-back plays don't inflate the count.

Output
------
analytics/outputs/quality_checks/prereleases.csv
"""

import sqlite3
import pandas as pd
from datetime import date
from pathlib import Path

DB_PATH = Path(__file__).resolve().parents[1] / "radio_plays.db"
QUALITY_DIR = Path(__file__).resolve().parent / "outputs" / "quality_checks"
QUALITY_DIR.mkdir(exist_ok=True)

_QUERY = """
SELECT
    STRFTIME('%Y-W%W', p.play_ts) AS week,
    ct.canonical_id,
    ct.norm_artist,
    ct.display_title,
    ct.spotify_album_release_date  AS release_date,
    ct.spotify_album_type,
    DATE(p.play_ts)                AS play_date,
    p.station_show
FROM plays p
JOIN plays_to_canonical ptc ON p.id = ptc.play_id
JOIN canonical_tracks   ct  ON ptc.canonical_id = ct.canonical_id
WHERE ct.spotify_status = 'SUCCESS'
  AND ct.spotify_album_release_date IS NOT NULL
  AND DATE(p.play_ts) < ct.spotify_album_release_date
ORDER BY p.play_ts
"""


def run_prereleases():
    print("=== Pre-Release Plays Report ===")
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(_QUERY, conn)
    conn.close()

    if df.empty:
        print("  No pre-release plays found.")
        out_path = QUALITY_DIR / "prereleases.csv"
        pd.DataFrame(columns=[
            "week", "canonical_id", "norm_artist", "display_title",
            "release_date", "days_pre_released", "play_count",
            "shows", "spotify_album_type", "still_pre_release",
        ]).to_csv(out_path, index=False, encoding="utf-8")
        print(f"  Saved: {out_path}")
        return

    df["play_date"]    = pd.to_datetime(df["play_date"])
    df["release_date"] = pd.to_datetime(df["release_date"])
    df["days_until_release"] = (df["release_date"] - df["play_date"]).dt.days

    grouped = (
        df.groupby(["week", "canonical_id"])
        .agg(
            norm_artist=("norm_artist",       "first"),
            display_title=("display_title",   "first"),
            release_date=("release_date",     "first"),
            spotify_album_type=("spotify_album_type", "first"),
            days_pre_released=("days_until_release",  "max"),
            play_count=("canonical_id",               "count"),
            shows=("station_show",
                   lambda x: "; ".join(sorted(x.dropna().unique()))),
        )
        .reset_index()
    )

    today = pd.Timestamp(date.today())
    grouped["still_pre_release"] = grouped["release_date"] > today
    grouped["release_date"] = grouped["release_date"].dt.strftime("%Y-%m-%d")

    grouped = grouped.sort_values(
        ["still_pre_release", "release_date", "week"],
        ascending=[False, True, True],
    ).reset_index(drop=True)

    out_cols = [
        "week", "canonical_id", "norm_artist", "display_title",
        "release_date", "days_pre_released", "play_count",
        "shows", "spotify_album_type", "still_pre_release",
    ]
    grouped = grouped[out_cols]

    out_path = QUALITY_DIR / "prereleases.csv"
    grouped.to_csv(out_path, index=False, encoding="utf-8")

    still_out = grouped["still_pre_release"].sum()
    print(f"  Total pre-release week/track rows : {len(grouped)}")
    print(f"  Unique tracks                     : {grouped['canonical_id'].nunique()}")
    print(f"  Still pre-release (as of today)   : {still_out}")
    print(f"  Saved: {out_path}")
    print()


if __name__ == "__main__":
    run_prereleases()
