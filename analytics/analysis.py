# analytics/analysis.py

import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path


DB_PATH = Path(__file__).resolve().parents[1] / "radio_plays.db"

OUTPUT_DIR = Path(__file__).resolve().parent / "outputs"
OUTPUT_DIR.mkdir(exist_ok=True)


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
        c.spotify_duration_ms
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
# SECTION 1 — STRUCTURAL METRICS
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
# SECTION 2 — ENRICHMENT METRICS
# -----------------------------

def average_album_year_by_show(df):
    result = (
        df.dropna(subset=["spotify_album_release_year"])
        .groupby("station_show")["spotify_album_release_year"]
        .mean()
        .reset_index(name="avg_album_year")
        .sort_values("avg_album_year", ascending=False)
    )
    return result


def freshness_percentage_by_show(df, recent_year_threshold=5):
    current_year = pd.Timestamp.now().year
    cutoff = current_year - recent_year_threshold

    df_recent = df[df["spotify_album_release_year"] >= cutoff]

    total_counts = df.groupby("station_show").size().reset_index(name="total_tracks")
    recent_counts = df_recent.groupby("station_show").size().reset_index(name="recent_tracks")

    merged = total_counts.merge(recent_counts, on="station_show", how="left")
    merged["recent_tracks"] = merged["recent_tracks"].fillna(0)

    merged["freshness_pct"] = merged["recent_tracks"] / merged["total_tracks"]

    return merged.sort_values("freshness_pct", ascending=False)


# -----------------------------
# MAIN EXECUTION
# -----------------------------

if __name__ == "__main__":
    df = load_base_dataset()

    # Export core analytics tables

    unique_artists_per_show(df).to_csv(OUTPUT_DIR / "analytics_unique_artists.csv", index=False)
    entropy_by_show(df).to_csv(OUTPUT_DIR / "analytics_entropy.csv", index=False)
    exclusive_artist_percentage(df).to_csv(OUTPUT_DIR / "analytics_exclusive_artists.csv", index=False)
    average_album_year_by_show(df).to_csv(OUTPUT_DIR / "analytics_avg_album_year.csv", index=False)
    freshness_percentage_by_show(df).to_csv(OUTPUT_DIR / "analytics_freshness.csv", index=False)
    unique_artists_per_hour(df).to_csv(OUTPUT_DIR / "analytics_unique_artists_per_hour.csv", index=False)
   
    print("\nUnique Artists Per Show")
    print(unique_artists_per_show(df))

    print("\nUnique Artists Per Hour")
    print(unique_artists_per_hour(df))

    print("\nArtist Entropy By Show")
    print(entropy_by_show(df))

    print("\nExclusive Artist Percentage")
    print(exclusive_artist_percentage(df))

    print("\nAverage Album Year By Show")
    print(average_album_year_by_show(df))

    print("\nFreshness Percentage By Show")
    print(freshness_percentage_by_show(df))