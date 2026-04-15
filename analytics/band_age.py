# analytics/band_age.py
# -*- coding: utf-8 -*-
"""
Band age at recording: best_year - mb_earliest_release_year.

Measures how far into their career an artist was when a played track was
recorded.  A show playing deep classic-rock back-catalogue will have high
band age; a new-music show will have low band age even if the tracks are
from recently established artists.

Coverage note: only tracks where canonical_artists.mb_artist_status =
'SUCCESS' are included.  Coverage % is reported per show so the reader
can judge how reliable each show's metric is.

Outputs (analytics/outputs/band_age/):
  boxplot_band_age.html        -- per-show boxplot sorted by median
  band_age_summary.csv         -- per-show mean, median, coverage stats
"""

import sys
import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path
import plotly.graph_objects as go

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from analytics.era_continuity import SEGMENT_SHOWS, get_inband_tracks

DB_PATH = Path(__file__).resolve().parents[1] / "radio_plays.db"

OUTPUT_DIR = Path(__file__).resolve().parent / "outputs"
BAND_AGE_DIR = OUTPUT_DIR / "band_age"
BAND_AGE_DIR.mkdir(parents=True, exist_ok=True)

BEST_YEAR_SQL = """
    CASE
        WHEN ct.manual_year_override IS NOT NULL
        THEN ct.manual_year_override
        WHEN ct.mb_isrc_year IS NOT NULL
         AND ct.mb_title_artist_year IS NOT NULL
         AND ct.mb_isrc_year < ct.spotify_album_release_year
         AND ct.mb_title_artist_year < ct.spotify_album_release_year
        THEN CASE WHEN ct.mb_isrc_year < ct.mb_title_artist_year
                  THEN ct.mb_isrc_year ELSE ct.mb_title_artist_year END
        WHEN ct.mb_isrc_year IS NOT NULL
         AND ct.mb_isrc_year < ct.spotify_album_release_year
        THEN ct.mb_isrc_year
        WHEN ct.mb_title_artist_year IS NOT NULL
         AND ct.mb_title_artist_year < ct.spotify_album_release_year
        THEN ct.mb_title_artist_year
        ELSE ct.spotify_album_release_year
    END
"""


def _load_data():
    """Load play-level data joined to canonical_tracks and canonical_artists."""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(f"""
        SELECT
            p.id          AS play_id,
            p.play_ts,
            p.station_show,
            ct.canonical_id,
            ct.display_artist,
            ct.display_title,
            ct.spotify_primary_artist_id,
            ca.mb_artist_status,
            ca.mb_earliest_release_year,
            ({BEST_YEAR_SQL}) AS best_year
        FROM plays p
        JOIN plays_to_canonical ptc ON p.id = ptc.play_id
        JOIN canonical_tracks ct   ON ptc.canonical_id = ct.canonical_id
        LEFT JOIN canonical_artists ca
               ON ct.spotify_primary_artist_id = ca.spotify_artist_id
        WHERE p.is_music_show = 1
          AND ct.spotify_status = 'SUCCESS'
    """, conn)
    conn.close()
    df["play_ts"] = pd.to_datetime(df["play_ts"], errors="coerce")
    return df


def _apply_segmentation(df):
    """Apply density-based segmentation for SEGMENT_SHOWS.

    Returns a single DataFrame with segmented shows labelled '<name> *'
    and all other shows unchanged.
    """
    df["play_date"] = df["play_ts"].dt.strftime("%Y-%m-%d")
    df["play_hour"] = df["play_ts"].dt.strftime("%H")

    seg_mask = df["station_show"].isin(SEGMENT_SHOWS)
    df_seg = get_inband_tracks(df[seg_mask].copy())
    df_seg["station_show"] = df_seg["station_show"] + " *"

    return pd.concat([df[~seg_mask], df_seg], ignore_index=True)


def _coverage_summary(df):
    """Per-show coverage: how many plays have MB artist data + best_year."""
    total = df.groupby("station_show")["play_id"].count().rename("n_total")
    covered = (
        df[
            (df["mb_artist_status"] == "SUCCESS")
            & df["best_year"].notna()
        ]
        .groupby("station_show")["play_id"]
        .count()
        .rename("n_covered")
    )
    cov = pd.concat([total, covered], axis=1).fillna(0)
    cov["coverage_pct"] = (cov["n_covered"] / cov["n_total"] * 100).round(1)
    return cov


def _compute_band_age(df):
    """Filter to covered tracks and compute band_age_at_recording."""
    covered = df[
        (df["mb_artist_status"] == "SUCCESS")
        & df["best_year"].notna()
        & df["mb_earliest_release_year"].notna()
    ].copy()
    covered["best_year"] = covered["best_year"].astype(int)
    covered["mb_earliest_release_year"] = covered["mb_earliest_release_year"].astype(int)
    covered["band_age"] = covered["best_year"] - covered["mb_earliest_release_year"]
    return covered


def _summary_csv(df_age, cov):
    """Build per-show summary stats and write CSV."""
    stats = (
        df_age.groupby("station_show")["band_age"]
        .agg(
            mean_band_age="mean",
            median_band_age="median",
            p25_band_age=lambda x: x.quantile(0.25),
            p75_band_age=lambda x: x.quantile(0.75),
            min_band_age="min",
            max_band_age="max",
        )
        .round(1)
        .reset_index()
    )
    out = stats.merge(cov.reset_index(), on="station_show", how="left")
    col_order = [
        "station_show", "n_total", "n_covered", "coverage_pct",
        "mean_band_age", "median_band_age",
        "p25_band_age", "p75_band_age",
        "min_band_age", "max_band_age",
    ]
    out = out[col_order].sort_values("median_band_age")
    out_path = BAND_AGE_DIR / "band_age_summary.csv"
    out.to_csv(out_path, index=False)
    print(f"  Saved: {out_path}")
    return out


def _boxplot(df_age, cov):
    """Plotly boxplot: band age at recording by show, sorted by median."""
    show_order = (
        df_age.groupby("station_show")["band_age"]
        .median()
        .sort_values()
        .index.tolist()
    )

    # Coverage label suffix for each show name on the x-axis
    cov_dict = cov["coverage_pct"].to_dict()

    fig = go.Figure()

    for show in show_order:
        ages = df_age[df_age["station_show"] == show]["band_age"]
        cov_pct = cov_dict.get(show, 0)
        label = f"{show}<br><sup>{cov_pct:.0f}% covered</sup>"
        fig.add_trace(go.Box(
            y=ages,
            name=label,
            boxpoints="outliers",
            marker_size=4,
        ))

    fig.update_layout(
        title="Band Age at Recording by Show<br>"
              "<sup>Years between artist's earliest release and track's best_year "
              "(MB-covered tracks only)</sup>",
        yaxis_title="Band age at recording (years)",
        xaxis_title="Show",
        showlegend=False,
        height=650,
        margin=dict(b=200),
        xaxis=dict(tickangle=-40),
        annotations=[dict(
            text="* = density-segmented tracks",
            xref="paper", yref="paper",
            x=0.0, y=-0.30,
            showarrow=False,
            font=dict(size=11, color="#666666"),
            xanchor="left",
        )],
    )

    out_path = BAND_AGE_DIR / "boxplot_band_age.html"
    fig.write_html(str(out_path))
    print(f"  Saved: {out_path}")


def run_band_age():
    print("=== Band Age at Recording ===")
    print()

    print("  Loading data...")
    df = _load_data()

    print("  Applying segmentation...")
    df = _apply_segmentation(df)

    # Coverage before filtering
    cov = _coverage_summary(df)

    total_plays = len(df)
    covered_plays = int(cov["n_covered"].sum())
    overall_pct = covered_plays / total_plays * 100 if total_plays else 0
    print(f"  Overall coverage: {covered_plays}/{total_plays} plays "
          f"({overall_pct:.1f}%) have MB artist data + best_year")
    print()

    print("  Computing band age...")
    df_age = _compute_band_age(df)

    print("  Writing summary CSV...")
    summary = _summary_csv(df_age, cov)

    print("  Writing boxplot...")
    _boxplot(df_age, cov)

    print()
    print("  --- Per-show summary (sorted by median band age) ---")
    print(f"  {'Show':<42} {'Coverage':>9} {'Median':>7} {'Mean':>7} {'P25':>6} {'P75':>6}")
    print("  " + "-" * 80)
    for _, row in summary.iterrows():
        print(
            f"  {row['station_show']:<42} "
            f"  {row['coverage_pct']:>6.1f}%"
            f"  {row['median_band_age']:>6.1f}yr"
            f"  {row['mean_band_age']:>6.1f}yr"
            f"  {row['p25_band_age']:>5.1f}yr"
            f"  {row['p75_band_age']:>5.1f}yr"
        )

    print()
    print("=== Done ===")


if __name__ == "__main__":
    run_band_age()
