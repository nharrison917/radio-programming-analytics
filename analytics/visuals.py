# analytics/visuals.py

import pandas as pd
import matplotlib.pyplot as plt
import sqlite3
from pathlib import Path

OUTPUT_DIR = Path(__file__).resolve().parent / "outputs"
OUTPUT_DIR.mkdir(exist_ok=True)


DB_PATH = Path(__file__).resolve().parents[1] / "radio_plays.db"


def get_connection():
    return sqlite3.connect(DB_PATH)


def load_dataset():
    query = """
    SELECT
        p.id AS play_id,
        p.play_ts,
        p.station_show,
        c.norm_artist AS normalized_artist,
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
    """

    conn = get_connection()
    df = pd.read_sql_query(query, conn)
    conn.close()

    df["play_ts"] = pd.to_datetime(df["play_ts"], errors="coerce")
    return df


def compute_unique_artists_per_hour(df):
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

    return merged[["station_show", "unique_artists_per_hour"]]


def compute_freshness(df, recent_year_threshold=5):
    current_year = pd.Timestamp.now().year
    cutoff = current_year - recent_year_threshold

    total_counts = (
        df.groupby("station_show")
        .size()
        .reset_index(name="total_tracks")
    )

    recent_counts = (
        df[df["best_year"] >= cutoff]
        .groupby("station_show")
        .size()
        .reset_index(name="recent_tracks")
    )

    merged = total_counts.merge(recent_counts, on="station_show", how="left")
    merged["recent_tracks"] = merged["recent_tracks"].fillna(0)
    merged["freshness_pct"] = merged["recent_tracks"] / merged["total_tracks"]

    return merged[["station_show", "freshness_pct"]]


def build_scatter_plot():
    df = load_dataset()

    diversity = compute_unique_artists_per_hour(df)
    freshness = compute_freshness(df)

    merged = diversity.merge(freshness, on="station_show")

    plt.figure(figsize=(11, 7))

    scatter = plt.scatter(
        merged["unique_artists_per_hour"],
        merged["freshness_pct"],
        c=merged["freshness_pct"],   # color by freshness
        cmap="viridis",
        s=140,
        alpha=0.85
    )

    cbar = plt.colorbar(scatter)
    cbar.set_label("Freshness (%)")

    cbar.ax.yaxis.set_major_formatter(
        plt.FuncFormatter(lambda y, _: f"{y:.0%}")
    )

    for _, row in merged.iterrows():
        plt.text(
            row["unique_artists_per_hour"] + 0.15,
            row["freshness_pct"] + 0.01,
            row["station_show"],
            fontsize=8
        )

    plt.xlabel("Unique Artists Per Broadcast Hour")
    plt.ylabel("Freshness (% Tracks Released in Last 5 Years)")
    plt.title(
        "Programming Density vs Contemporary Bias\n(Unique Artists per Hour vs % Released in Last 5 Years)"
    )    
    plt.grid(alpha=0.3)

    plt.gca().yaxis.set_major_formatter(
        plt.FuncFormatter(lambda y, _: f"{y:.0%}")
    )

    plt.tight_layout()

    output_path = OUTPUT_DIR / "density_vs_freshness.png"
    plt.savefig(output_path, dpi=300)
    plt.close()
    print(f"Saved plot to: {output_path}")


def run_visuals():
    build_scatter_plot()

    from analytics.boxplot_release_year import build_release_year_boxplot
    build_release_year_boxplot()


if __name__ == "__main__":
    run_visuals()