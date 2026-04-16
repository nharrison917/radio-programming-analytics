# analytics/band_age.py
# -*- coding: utf-8 -*-
"""
Band age at recording: best_year - career_start_year.

Measures how far into their career an artist was when a played track was
recorded.  A show playing deep classic-rock back-catalogue will have high
band age; a new-music show will have low band age even if the tracks are
from recently established artists.

Career start year resolution (per artist):
  1. mb_earliest_release_year  -- when mb_artist_status = 'SUCCESS' (best:
     covers pre-streaming career via full release-group browse)
  2. earliest_release_year     -- Spotify fallback when MB has no data
     (NO_MATCH, FAILED, or NULL); may understate career length for older
     artists whose full back-catalogue is not on Spotify

Coverage is reported per show as MB% and Spotify% so the reader can judge
how reliable each show's metric is.

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
QUALITY_DIR = OUTPUT_DIR / "quality_checks"
QUALITY_DIR.mkdir(parents=True, exist_ok=True)

BAND_AGE_NEG_THRESHOLD = -2
BAND_AGE_POS_THRESHOLD = 50

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
            ct.spotify_album_release_year,
            ct.mb_isrc_year,
            ct.mb_title_artist_year,
            ca.mb_artist_status,
            ca.mb_earliest_release_year,
            ca.earliest_release_year,
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
    """Per-show coverage split by career-start source (MB vs Spotify fallback)."""
    total = df.groupby("station_show")["play_id"].count().rename("n_total")

    mb_mask = (
        (df["mb_artist_status"] == "SUCCESS")
        & df["mb_earliest_release_year"].notna()
        & df["best_year"].notna()
    )
    sp_mask = (
        ~mb_mask
        & df["earliest_release_year"].notna()
        & df["best_year"].notna()
    )

    n_mb = (
        df[mb_mask].groupby("station_show")["play_id"].count().rename("n_mb")
    )
    n_sp = (
        df[sp_mask].groupby("station_show")["play_id"].count().rename("n_spotify")
    )

    cov = pd.concat([total, n_mb, n_sp], axis=1).fillna(0)
    cov["n_covered"] = cov["n_mb"] + cov["n_spotify"]
    cov["coverage_pct"] = (cov["n_covered"] / cov["n_total"] * 100).round(1)
    cov["mb_pct"]      = (cov["n_mb"]      / cov["n_total"] * 100).round(1)
    cov["spotify_pct"] = (cov["n_spotify"] / cov["n_total"] * 100).round(1)
    return cov


def _compute_band_age(df):
    """Resolve career_start_year and compute band_age_at_recording.

    Resolution order per play:
      1. mb_earliest_release_year  (mb_artist_status = 'SUCCESS')
      2. earliest_release_year     (Spotify fallback)
    Plays with neither are excluded.
    """
    df = df.copy()

    mb_mask = (
        (df["mb_artist_status"] == "SUCCESS")
        & df["mb_earliest_release_year"].notna()
    )
    sp_mask = ~mb_mask & df["earliest_release_year"].notna()

    df["career_start_year"]   = np.nan
    df["career_start_source"] = None

    df.loc[mb_mask, "career_start_year"]   = df.loc[mb_mask, "mb_earliest_release_year"]
    df.loc[mb_mask, "career_start_source"] = "mb"
    df.loc[sp_mask, "career_start_year"]   = df.loc[sp_mask, "earliest_release_year"]
    df.loc[sp_mask, "career_start_source"] = "spotify"

    covered = df[df["career_start_year"].notna() & df["best_year"].notna()].copy()
    covered["career_start_year"] = covered["career_start_year"].astype(int)
    covered["best_year"]         = covered["best_year"].astype(int)
    covered["band_age"]          = covered["best_year"] - covered["career_start_year"]
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
        "station_show", "n_total", "n_mb", "n_spotify", "n_covered",
        "coverage_pct", "mb_pct", "spotify_pct",
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

    cov_pct_dict = cov["coverage_pct"].to_dict()
    mb_pct_dict  = cov["mb_pct"].to_dict()

    fig = go.Figure()

    for show in show_order:
        ages    = df_age[df_age["station_show"] == show]["band_age"]
        cov_pct = cov_pct_dict.get(show, 0)
        mb_pct  = mb_pct_dict.get(show, 0)
        label   = f"{show}<br><sup>{cov_pct:.0f}% covered ({mb_pct:.0f}% MB)</sup>"
        fig.add_trace(go.Box(
            y=ages,
            name=label,
            boxpoints="outliers",
            marker_size=4,
        ))

    fig.update_layout(
        title="Band Age at Recording by Show<br>"
              "<sup>Years between artist's earliest release and track's best_year. "
              "MB source preferred; Spotify earliest year used as fallback.</sup>",
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


def _write_band_age_quality_reports(df_age):
    """Write quality CSVs for band_age outliers: negatives and extreme positives."""
    per_track = (
        df_age.groupby("canonical_id")
        .agg(
            display_artist=("display_artist", "first"),
            display_title=("display_title", "first"),
            best_year=("best_year", "first"),
            career_start_year=("career_start_year", "first"),
            career_start_source=("career_start_source", "first"),
            band_age=("band_age", "first"),
            mb_artist_status=("mb_artist_status", "first"),
            spotify_album_release_year=("spotify_album_release_year", "first"),
            mb_isrc_year=("mb_isrc_year", "first"),
            mb_title_artist_year=("mb_title_artist_year", "first"),
            play_count=("play_id", "count"),
        )
        .reset_index()
    )

    col_order = [
        "canonical_id", "display_artist", "display_title",
        "best_year", "career_start_year", "career_start_source", "band_age",
        "play_count", "mb_artist_status",
        "spotify_album_release_year", "mb_isrc_year", "mb_title_artist_year",
    ]

    neg = per_track[per_track["band_age"] < BAND_AGE_NEG_THRESHOLD].sort_values("band_age")
    neg_path = QUALITY_DIR / "band_age_negative.csv"
    neg[col_order].to_csv(neg_path, index=False)
    print(f"  Saved: {neg_path} ({len(neg)} tracks)")

    pos = per_track[per_track["band_age"] > BAND_AGE_POS_THRESHOLD].sort_values(
        "band_age", ascending=False
    )
    pos_path = QUALITY_DIR / "band_age_extreme.csv"
    pos[col_order].to_csv(pos_path, index=False)
    print(f"  Saved: {pos_path} ({len(pos)} tracks)")


def run_band_age():
    print("=== Band Age at Recording ===")
    print()

    print("  Loading data...")
    df = _load_data()

    print("  Applying segmentation...")
    df = _apply_segmentation(df)

    # Coverage before filtering
    cov = _coverage_summary(df)

    total_plays   = len(df)
    n_mb          = int(cov["n_mb"].sum())
    n_spotify     = int(cov["n_spotify"].sum())
    n_covered     = n_mb + n_spotify
    overall_pct   = n_covered / total_plays * 100 if total_plays else 0
    mb_pct        = n_mb      / total_plays * 100 if total_plays else 0
    spotify_pct   = n_spotify / total_plays * 100 if total_plays else 0
    print(f"  Overall coverage : {n_covered}/{total_plays} plays ({overall_pct:.1f}%)")
    print(f"    MB source      : {n_mb} ({mb_pct:.1f}%)")
    print(f"    Spotify source : {n_spotify} ({spotify_pct:.1f}%)")
    print()

    print("  Computing band age...")
    df_age = _compute_band_age(df)

    print("  Writing summary CSV...")
    summary = _summary_csv(df_age, cov)

    print("  Writing boxplot...")
    _boxplot(df_age, cov)

    print("  Writing quality reports...")
    _write_band_age_quality_reports(df_age)

    print()
    print("  --- Per-show summary (sorted by median band age) ---")
    print(f"  {'Show':<42} {'Cvd%':>5} {'MB%':>5} {'SP%':>5} {'Median':>7} {'Mean':>7} {'P25':>6} {'P75':>6}")
    print("  " + "-" * 90)
    for _, row in summary.iterrows():
        print(
            f"  {row['station_show']:<42} "
            f"  {row['coverage_pct']:>4.0f}%"
            f"  {row['mb_pct']:>4.0f}%"
            f"  {row['spotify_pct']:>4.0f}%"
            f"  {row['median_band_age']:>6.1f}yr"
            f"  {row['mean_band_age']:>6.1f}yr"
            f"  {row['p25_band_age']:>5.1f}yr"
            f"  {row['p75_band_age']:>5.1f}yr"
        )

    print()
    print("=== Done ===")


if __name__ == "__main__":
    run_band_age()
