# analytics/wednesday_freshness.py
#
# 107.1 The Peak publicly states that Wednesdays feature at least one new song
# per broadcast hour. This module tests whether that claim is reflected in the
# data -- and whether Wednesday programming has a measurably higher bias toward
# recently-released tracks relative to other days of the week.
#
# "New" is defined relative to the week the play occurred (rolling window),
# not an absolute date -- so the analysis stays valid as the dataset ages.
#
# A 14-day forward buffer is applied to account for confirmed pre-release
# advance plays in the dataset (max observed gap: 10 days).
#
# Outputs:
#   analytics/outputs/wednesday_freshness.html  -- Plotly interactive report

import sqlite3
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from scraper.config import DB_PATH


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

FORWARD_BUFFER_DAYS = 14   # plays up to 14 days before release date count as "new"
THRESHOLDS_WEEKS = [8, 16, 24]

BIASED_SHOWS = {
    "This Just In with Meg White",
    "10 @ 10",
    "10 @ 10 Weekend Replay",
    "90's at Night",
}

WEEKDAY_ORDER = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

OUTPUT_PATH = Path(__file__).resolve().parent / "outputs" / "freshness" / "wednesday_freshness.html"
OUTPUT_PATH.parent.mkdir(exist_ok=True)


# ---------------------------------------------------------------------------
# Data load
# ---------------------------------------------------------------------------

def load_plays():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("""
        SELECT
            p.play_ts,
            p.station_show,
            c.spotify_album_release_date,
            c.spotify_album_release_date_precision
        FROM plays p
        JOIN plays_to_canonical ptc ON p.id = ptc.play_id
        JOIN canonical_tracks c ON ptc.canonical_id = c.canonical_id
        WHERE c.spotify_status = 'SUCCESS'
          AND c.spotify_album_release_date IS NOT NULL
          AND c.spotify_album_release_date_precision = 'day'
          AND p.is_music_show = 1
    """, conn)
    conn.close()

    df["play_ts"] = pd.to_datetime(df["play_ts"])
    df["release_date"] = pd.to_datetime(df["spotify_album_release_date"])
    df["day_of_week"] = pd.Categorical(
        df["play_ts"].dt.day_name(),
        categories=WEEKDAY_ORDER,
        ordered=True
    )
    df["hour"] = df["play_ts"].dt.hour
    df["play_date"] = df["play_ts"].dt.normalize()

    # Negative = pre-release (play happened before release date)
    df["days_since_release"] = (df["play_date"] - df["release_date"]).dt.days

    return df


# ---------------------------------------------------------------------------
# Freshness flag
# ---------------------------------------------------------------------------

def flag_new(df, threshold_weeks):
    threshold_days = threshold_weeks * 7
    # "New" = released within threshold_days before play, OR within FORWARD_BUFFER_DAYS after play
    is_new = (
        (df["days_since_release"] >= -FORWARD_BUFFER_DAYS) &
        (df["days_since_release"] <= threshold_days)
    )
    return df.assign(is_new=is_new)


# ---------------------------------------------------------------------------
# Metric 1: % of plays that are "new", by day-of-week
# ---------------------------------------------------------------------------

def freshness_pct_by_day(df, threshold_weeks):
    df = flag_new(df, threshold_weeks)
    result = (
        df.groupby("day_of_week", observed=True)["is_new"]
        .mean()
        .mul(100)
        .round(1)
        .reindex(WEEKDAY_ORDER)
    )
    return result


# ---------------------------------------------------------------------------
# Metric 2: % of hours containing at least one "new" track, by day-of-week
# (directly tests the station's "at least one new song per hour" claim)
# ---------------------------------------------------------------------------

def hours_with_new_pct_by_day(df, threshold_weeks):
    df = flag_new(df, threshold_weeks)
    # Per hour slot: did any play qualify as new?
    per_hour = (
        df.groupby(["day_of_week", "play_date", "hour"], observed=True)["is_new"]
        .any()
        .reset_index()
    )
    result = (
        per_hour.groupby("day_of_week", observed=True)["is_new"]
        .mean()
        .mul(100)
        .round(1)
        .reindex(WEEKDAY_ORDER)
    )
    return result


# ---------------------------------------------------------------------------
# Build figure
# ---------------------------------------------------------------------------

THRESHOLD_COLORS = {
    8:  "#1f77b4",
    16: "#ff7f0e",
    24: "#2ca02c",
}

# Wednesdays get a distinct marker symbol to make them easy to spot
MARKER_SYMBOLS = ["circle"] * 7
MARKER_SYMBOLS[WEEKDAY_ORDER.index("Wednesday")] = "star"


def make_traces(df, metric_fn, threshold_list):
    traces = []
    for weeks in threshold_list:
        series = metric_fn(df, weeks)
        traces.append(go.Bar(
            name=f"{weeks}w",
            x=series.index.tolist(),
            y=series.values,
            marker_color=[
                "#e63946" if day == "Wednesday" else THRESHOLD_COLORS[weeks]
                for day in series.index
            ],
            showlegend=True,
        ))
    return traces


def build_figure(df_all, df_excl):
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=[
            "% Plays 'New' -- All Shows",
            "% Plays 'New' -- Excl. Format Shows",
            "% Hours w/ >=1 New Track<br>All Shows",
            "% Hours w/ >=1 New Track<br>Excl. Format Shows",
        ],
        vertical_spacing=0.22,
        horizontal_spacing=0.14,
    )

    configs = [
        (1, 1, df_all,  freshness_pct_by_day),
        (1, 2, df_excl, freshness_pct_by_day),
        (2, 1, df_all,  hours_with_new_pct_by_day),
        (2, 2, df_excl, hours_with_new_pct_by_day),
    ]

    for row, col, df, metric_fn in configs:
        traces = make_traces(df, metric_fn, THRESHOLDS_WEEKS)
        first = (row == 1 and col == 1)
        for trace in traces:
            trace.showlegend = first
            fig.add_trace(trace, row=row, col=col)

    # Wednesday reference line on all subplots
    wed_x = "Wednesday"
    for row in [1, 2]:
        for col in [1, 2]:
            fig.add_vline(
                x=wed_x,
                line_dash="dot",
                line_color="rgba(230,57,70,0.4)",
                row=row, col=col
            )

    fig.update_layout(
        title=dict(
            text=(
                "Wednesday Freshness Analysis<br>"
                "<sup>Does Wednesday programming have a measurably higher bias toward recently-released tracks?</sup>"
            ),
            y=0.98,
            x=0.5,
            xanchor="center",
            yanchor="top",
        ),
        barmode="group",
        height=900,
        margin=dict(t=100, b=100),
        legend_title="Threshold",
        legend=dict(orientation="h", yanchor="top", y=-0.08, xanchor="center", x=0.5),
        plot_bgcolor="white",
        paper_bgcolor="white",
    )

    # Y-axis labels
    for axis in ["yaxis", "yaxis2", "yaxis3", "yaxis4"]:
        fig.update_layout(**{axis: dict(title="Percent (%)", gridcolor="#eeeeee", range=[0, 100])})

    # Shorten x-axis labels to 3-letter day abbreviations
    for axis in ["xaxis", "xaxis2", "xaxis3", "xaxis4"]:
        fig.update_layout(**{axis: dict(
            tickvals=WEEKDAY_ORDER,
            ticktext=["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        )})

    return fig


# ---------------------------------------------------------------------------
# Print summary table to terminal
# ---------------------------------------------------------------------------

def print_summary(df_all, df_excl):
    print("\n=== Wednesday Freshness Summary ===\n")
    for weeks in THRESHOLDS_WEEKS:
        series_all  = freshness_pct_by_day(df_all,  weeks)
        series_excl = freshness_pct_by_day(df_excl, weeks)
        wed_all  = series_all["Wednesday"]
        wed_excl = series_excl["Wednesday"]
        max_other_all  = series_all.drop("Wednesday").max()
        max_other_excl = series_excl.drop("Wednesday").max()
        print(f"Threshold {weeks}w:")
        print(f"  All shows  -- Wed: {wed_all:.1f}%  max other day: {max_other_all:.1f}%")
        print(f"  Excl shows -- Wed: {wed_excl:.1f}%  max other day: {max_other_excl:.1f}%")
    print()

    print("=== Hours with >=1 New Track ===\n")
    for weeks in THRESHOLDS_WEEKS:
        series_all  = hours_with_new_pct_by_day(df_all,  weeks)
        series_excl = hours_with_new_pct_by_day(df_excl, weeks)
        wed_all  = series_all["Wednesday"]
        wed_excl = series_excl["Wednesday"]
        max_other_all  = series_all.drop("Wednesday").max()
        max_other_excl = series_excl.drop("Wednesday").max()
        print(f"Threshold {weeks}w:")
        print(f"  All shows  -- Wed: {wed_all:.1f}%  max other day: {max_other_all:.1f}%")
        print(f"  Excl shows -- Wed: {wed_excl:.1f}%  max other day: {max_other_excl:.1f}%")
    print()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run_wednesday_freshness():
    print("Loading play data...")
    df_all = load_plays()
    df_excl = df_all[~df_all["station_show"].isin(BIASED_SHOWS)].copy()

    print(f"  Total qualifying plays (day-precision): {len(df_all)}")
    print(f"  After excluding format shows:           {len(df_excl)}")

    print_summary(df_all, df_excl)

    print("Building visualisation...")
    fig = build_figure(df_all, df_excl)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    fig.write_html(str(OUTPUT_PATH))
    print(f"Saved: {OUTPUT_PATH}")


if __name__ == "__main__":
    run_wednesday_freshness()
