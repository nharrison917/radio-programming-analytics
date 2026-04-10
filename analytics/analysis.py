# analytics/analysis.py
# -*- coding: utf-8 -*-

import sys
import sqlite3
import logging
import pandas as pd
import numpy as np
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scraper.utils import setup_logging, rotate_logs
from analytics.visuals import run_visuals
from analytics.wednesday_freshness import run_wednesday_freshness

DB_PATH = Path(__file__).resolve().parents[1] / "radio_plays.db"

OUTPUT_DIR = Path(__file__).resolve().parent / "outputs"
OUTPUT_DIR.mkdir(exist_ok=True)

LOG_DIR = Path(__file__).resolve().parents[1] / "logs"


def get_connection():
    return sqlite3.connect(DB_PATH)


def load_base_dataset():
    """
    Loads play-level data joined to canonical and Spotify enrichment.
    """
    query = """
    SELECT
        p.id AS play_id,
        p.play_ts,
        p.station_show,
        c.canonical_id,
        c.norm_artist AS normalized_artist,
        c.display_title AS normalized_title,
        c.spotify_album_release_year,
        CASE
            WHEN c.manual_year_override IS NOT NULL
            THEN c.manual_year_override
            WHEN c.mb_isrc_year IS NOT NULL
             AND c.mb_title_artist_year IS NOT NULL
             AND c.mb_isrc_year < c.spotify_album_release_year
             AND c.mb_title_artist_year < c.spotify_album_release_year
            THEN CASE WHEN c.mb_isrc_year < c.mb_title_artist_year
                      THEN c.mb_isrc_year ELSE c.mb_title_artist_year END
            WHEN c.mb_isrc_year IS NOT NULL
             AND c.mb_isrc_year < c.spotify_album_release_year
            THEN c.mb_isrc_year
            WHEN c.mb_title_artist_year IS NOT NULL
             AND c.mb_title_artist_year < c.spotify_album_release_year
            THEN c.mb_title_artist_year
            ELSE c.spotify_album_release_year
        END AS best_year,
        c.spotify_duration_ms,
        c.spotify_status,
        c.mb_lookup_status,
        c.mb_ta_status
    FROM plays p
    JOIN plays_to_canonical pc ON p.id = pc.play_id
    JOIN canonical_tracks c ON pc.canonical_id = c.canonical_id
    WHERE p.is_music_show = 1
    """

    conn = get_connection()
    df = pd.read_sql_query(query, conn)
    conn.close()

    df["play_ts"] = pd.to_datetime(df["play_ts"], errors="coerce")

    return df


# -----------------------------
# SECTION 1 - STRUCTURAL METRICS
# -----------------------------

def unique_artists_per_show(df):
    result = (
        df.groupby("station_show")["normalized_artist"]
        .nunique()
        .reset_index(name="unique_artists")
        .sort_values("unique_artists", ascending=False)
    )
    return result


def unique_artists_per_hour(df):
    """
    Normalizes unique artist count by actual broadcast hours
    using distinct play hours.
    """
    df = df.copy()
    df["play_hour"] = df["play_ts"].dt.floor("h")

    broadcast_hours = (
        df.groupby("station_show")["play_hour"]
        .nunique()
        .reset_index(name="broadcast_hours")
    )

    unique_artists = (
        df.groupby("station_show")["normalized_artist"]
        .nunique()
        .reset_index(name="unique_artists")
    )

    merged = unique_artists.merge(broadcast_hours, on="station_show")

    merged["unique_artists_per_hour"] = (
        merged["unique_artists"] / merged["broadcast_hours"]
    )

    return merged.sort_values("unique_artists_per_hour", ascending=False)


def shannon_entropy(series):
    probs = series.value_counts(normalize=True)
    return -np.sum(probs * np.log2(probs))


def entropy_by_show(df):
    entropy_values = []

    for show, group in df.groupby("station_show"):
        entropy = shannon_entropy(group["normalized_artist"])
        entropy_values.append((show, entropy))

    result = pd.DataFrame(entropy_values, columns=["station_show", "artist_entropy"])
    return result.sort_values("artist_entropy", ascending=False)


def exclusive_artist_percentage(df):
    artist_show_counts = (
        df.groupby("normalized_artist")["station_show"]
        .nunique()
        .reset_index(name="show_count")
    )

    exclusive_artists = artist_show_counts[artist_show_counts["show_count"] == 1]

    exclusive_df = df[df["normalized_artist"].isin(exclusive_artists["normalized_artist"])]

    result = (
        exclusive_df.groupby("station_show")["normalized_artist"]
        .nunique()
        .reset_index(name="exclusive_artists")
    )

    total_artists = (
        df.groupby("station_show")["normalized_artist"]
        .nunique()
        .reset_index(name="total_artists")
    )

    merged = result.merge(total_artists, on="station_show")
    merged["exclusive_artist_pct"] = merged["exclusive_artists"] / merged["total_artists"]

    return merged.sort_values("exclusive_artist_pct", ascending=False)


# -----------------------------
# SECTION 2 - ENRICHMENT METRICS
# -----------------------------

def average_album_year_by_show(df):
    result = (
        df.dropna(subset=["best_year"])
        .groupby("station_show")["best_year"]
        .mean()
        .reset_index(name="avg_album_year")
        .sort_values("avg_album_year", ascending=False)
    )
    return result


def freshness_percentage_by_show(df, recent_year_threshold=5):
    current_year = pd.Timestamp.now().year
    cutoff = current_year - recent_year_threshold

    df_recent = df[df["best_year"] >= cutoff]

    total_counts = df.groupby("station_show").size().reset_index(name="total_tracks")
    recent_counts = df_recent.groupby("station_show").size().reset_index(name="recent_tracks")

    merged = total_counts.merge(recent_counts, on="station_show", how="left")
    merged["recent_tracks"] = merged["recent_tracks"].fillna(0)
    merged["freshness_pct"] = merged["recent_tracks"] / merged["total_tracks"]

    return merged.sort_values("freshness_pct", ascending=False)


# -----------------------------
# SECTION 3 - ARTIST BREADTH
# -----------------------------

def artist_breadth(df):
    """
    Global artist breadth: how many distinct songs each artist has had played,
    across all shows combined.

    Columns returned:
      normalized_artist  - artist key
      unique_songs       - distinct canonical tracks played
      total_plays        - total play events
      repeat_ratio       - total_plays / unique_songs (higher = more rotation on fewer songs)
      show_count         - number of distinct shows the artist appeared on
    """
    unique_songs = (
        df.groupby("normalized_artist")["canonical_id"]
        .nunique()
        .reset_index(name="unique_songs")
    )

    total_plays = (
        df.groupby("normalized_artist")["play_id"]
        .count()
        .reset_index(name="total_plays")
    )

    show_count = (
        df.groupby("normalized_artist")["station_show"]
        .nunique()
        .reset_index(name="show_count")
    )

    result = unique_songs.merge(total_plays, on="normalized_artist")
    result = result.merge(show_count, on="normalized_artist")
    result["repeat_ratio"] = (result["total_plays"] / result["unique_songs"]).round(2)

    return result.sort_values("unique_songs", ascending=False)


# -----------------------------
# SECTION 4 - WEEKLY FRESH TRACKS
# -----------------------------

def top_fresh_tracks_by_week(window_months=12, top_n=5):
    """
    For each ISO week in the dataset, returns the top N most-played tracks
    whose Spotify release date falls within the last window_months.

    Uses a dedicated query since spotify_album_release_date is not in
    load_base_dataset().
    """
    query = """
    SELECT
        p.play_ts,
        c.canonical_id,
        c.norm_artist,
        c.display_title,
        CASE
            WHEN c.manual_year_override IS NOT NULL
            THEN c.manual_year_override
            WHEN c.mb_isrc_year IS NOT NULL
             AND c.mb_title_artist_year IS NOT NULL
             AND c.mb_isrc_year < c.spotify_album_release_year
             AND c.mb_title_artist_year < c.spotify_album_release_year
            THEN CASE WHEN c.mb_isrc_year < c.mb_title_artist_year
                      THEN c.mb_isrc_year ELSE c.mb_title_artist_year END
            WHEN c.mb_isrc_year IS NOT NULL
             AND c.mb_isrc_year < c.spotify_album_release_year
            THEN c.mb_isrc_year
            WHEN c.mb_title_artist_year IS NOT NULL
             AND c.mb_title_artist_year < c.spotify_album_release_year
            THEN c.mb_title_artist_year
            ELSE c.spotify_album_release_year
        END AS best_year
    FROM plays p
    JOIN plays_to_canonical pc ON p.id = pc.play_id
    JOIN canonical_tracks c ON pc.canonical_id = c.canonical_id
    WHERE p.is_music_show = 1
      AND c.spotify_status = 'SUCCESS'
      AND c.mb_lookup_status IS NOT NULL
      AND c.mb_ta_status IS NOT NULL
      AND c.spotify_album_release_date >= DATE('now', :cutoff)
    """

    conn = get_connection()
    df = pd.read_sql_query(query, conn, params={"cutoff": f"-{window_months} months"})
    conn.close()

    if df.empty:
        return {}

    df["play_ts"] = pd.to_datetime(df["play_ts"], errors="coerce")
    df["iso_week"] = df["play_ts"].dt.strftime("%Y-W%W")

    results = {}

    for week, group in df.groupby("iso_week"):
        top = (
            group.groupby(["canonical_id", "norm_artist", "display_title", "best_year"])
            .size()
            .reset_index(name="play_count")
            .sort_values("play_count", ascending=False)
            .head(top_n)
        )
        results[week] = top.reset_index(drop=True)

    return results


def print_fresh_tracks_report(results):
    print(f"\n--- Fresh Tracks Report (released in last 12 months) ---")

    if not results:
        print("No qualifying tracks found.")
        return

    for week in sorted(results.keys()):
        print(f"\nWeek {week}")
        for i, row in results[week].iterrows():
            year = int(row["best_year"]) if pd.notna(row["best_year"]) else "?"
            print(f"  {i + 1}. {row['norm_artist'].title()} - {row['display_title']} ({year}) - {int(row['play_count'])} plays")


# -----------------------------
# MAIN EXECUTION
# -----------------------------

def run_analysis():
    setup_logging("analysis")
    logging.info("Starting analysis pipeline")

    df = load_base_dataset()
    logging.info(f"Loaded {len(df)} play records")

    df_year = df[
        (df["spotify_status"] == "SUCCESS")
        & df["mb_lookup_status"].notna()
        & df["mb_ta_status"].notna()
    ].copy()
    logging.info(f"Year-quality subset: {len(df_year)} records (fully enriched)")

    # --- Structural metrics ---
    ua = unique_artists_per_show(df)
    uah = unique_artists_per_hour(df)
    ent = entropy_by_show(df)
    exc = exclusive_artist_percentage(df)

    # --- Enrichment metrics (year-quality subset only) ---
    aay = average_album_year_by_show(df_year)
    fresh = freshness_percentage_by_show(df_year)

    # --- Artist breadth ---
    breadth = artist_breadth(df)

    # Export CSVs
    ua.to_csv(OUTPUT_DIR / "analytics_unique_artists.csv", index=False)
    uah.to_csv(OUTPUT_DIR / "analytics_unique_artists_per_hour.csv", index=False)
    ent.to_csv(OUTPUT_DIR / "analytics_entropy.csv", index=False)
    exc.to_csv(OUTPUT_DIR / "analytics_exclusive_artists.csv", index=False)
    aay.to_csv(OUTPUT_DIR / "analytics_avg_album_year.csv", index=False)
    fresh.to_csv(OUTPUT_DIR / "analytics_freshness.csv", index=False)
    breadth.to_csv(OUTPUT_DIR / "analytics_artist_breadth.csv", index=False)

    logging.info("CSVs exported to analytics/outputs/")

    # Print results
    logging.info("---- Unique Artists Per Show ----")
    print("\nUnique Artists Per Show")
    print(ua.to_string(index=False))

    logging.info("---- Unique Artists Per Hour ----")
    print("\nUnique Artists Per Hour")
    print(uah.to_string(index=False))

    logging.info("---- Artist Entropy By Show ----")
    print("\nArtist Entropy By Show")
    print(ent.to_string(index=False))

    logging.info("---- Exclusive Artist Percentage ----")
    print("\nExclusive Artist Percentage")
    print(exc.to_string(index=False))

    logging.info("---- Average Album Year By Show ----")
    print("\nAverage Album Year By Show")
    print(aay.to_string(index=False))

    logging.info("---- Freshness Percentage By Show ----")
    print("\nFreshness Percentage By Show")
    print(fresh.to_string(index=False))

    logging.info("---- Artist Breadth (Top 20) ----")
    print("\nArtist Breadth - Top 20 by Unique Songs")
    print(breadth.head(20).to_string(index=False))

    # --- Weekly fresh tracks ---
    logging.info("---- Fresh Tracks Report ----")
    fresh_results = top_fresh_tracks_by_week()
    print_fresh_tracks_report(fresh_results)

    if fresh_results:
        fresh_rows = []
        for week, df_week in sorted(fresh_results.items()):
            df_week = df_week.copy()
            df_week.insert(0, "week", week)
            df_week["rank"] = range(1, len(df_week) + 1)
            fresh_rows.append(df_week)
        pd.concat(fresh_rows, ignore_index=True).to_csv(
            OUTPUT_DIR / "analytics_fresh_tracks.csv", index=False
        )
        logging.info("Fresh tracks CSV exported")

    # --- Visuals ---
    logging.info("---- Generating Visuals ----")
    run_visuals()

    # --- Wednesday freshness ---
    logging.info("---- Wednesday Freshness Analysis ----")
    run_wednesday_freshness()

    logging.info("Analysis pipeline complete")

    rotate_logs(LOG_DIR, prefix="analysis", max_logs=5)


if __name__ == "__main__":
    run_analysis()
