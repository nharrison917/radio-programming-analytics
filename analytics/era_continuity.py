# -*- coding: utf-8 -*-
"""
Era continuity analysis -- consecutive-pair release year metrics by show.

For each show, tracks are ordered chronologically within each show airing
(station_show + date). Consecutive pairs are scored on three metrics derived
from the absolute difference in album release years.

Outputs
-------
analytics/outputs/era_continuity.csv
analytics/outputs/era_continuity_mean_gap.html
analytics/outputs/era_continuity_fingerprint.html
analytics/outputs/era_continuity_buckets.html
"""

import sqlite3
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from pathlib import Path

# ---------------------------------------------------------------------------
# Thresholds -- adjust here
# ---------------------------------------------------------------------------
CONTINUITY_THRESHOLD = 3   # years: pairs <= this are "era-continuous"
BREAK_THRESHOLD = 10       # years: pairs > this are "era breaks"
MIN_PAIRS = 20             # minimum pairs for a show to appear in output
# ---------------------------------------------------------------------------

DB_PATH = Path(__file__).resolve().parents[1] / "radio_plays.db"
OUTPUT_DIR = Path(__file__).resolve().parent / "outputs"
OUTPUT_DIR.mkdir(exist_ok=True)

PAIRS_SQL = """
WITH ranked AS (
    SELECT
        p.play_ts,
        p.station_show,
        DATE(p.play_ts)  AS play_date,
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
        END AS yr,
        ROW_NUMBER() OVER (
            PARTITION BY p.station_show, DATE(p.play_ts)
            ORDER BY p.play_ts
        ) AS rn
    FROM plays p
    JOIN plays_to_canonical ptc ON p.id = ptc.play_id
    JOIN canonical_tracks   ct  ON ptc.canonical_id = ct.canonical_id
    WHERE ct.spotify_album_release_year IS NOT NULL
),
pairs AS (
    SELECT
        r1.station_show,
        ABS(r2.yr - r1.yr)       AS gap,
        (r1.yr + r2.yr) / 2.0   AS mid_era
    FROM ranked r1
    JOIN ranked r2
        ON  r1.station_show = r2.station_show
        AND r1.play_date    = r2.play_date
        AND r2.rn           = r1.rn + 1
)
SELECT
    station_show,
    COUNT(*)                                                              AS total_pairs,
    ROUND(AVG(gap), 2)                                                    AS mean_abs_gap,
    ROUND(100.0 * SUM(CASE WHEN gap <= {ct} THEN 1 ELSE 0 END)
          / COUNT(*), 1)                                                  AS era_continuity_pct,
    ROUND(100.0 * SUM(CASE WHEN gap > {bt} THEN 1 ELSE 0 END)
          / COUNT(*), 1)                                                  AS era_break_pct,
    SUM(CASE WHEN gap <= {ct} THEN 1 ELSE 0 END)                         AS tight_pairs,
    SUM(CASE WHEN gap > {ct} AND gap <= {bt} THEN 1 ELSE 0 END)         AS mid_pairs,
    SUM(CASE WHEN gap > {bt} THEN 1 ELSE 0 END)                         AS break_pairs,
    ROUND(AVG(mid_era), 0)                                               AS avg_era
FROM pairs
GROUP BY station_show
HAVING COUNT(*) >= {mp}
ORDER BY mean_abs_gap
""".format(ct=CONTINUITY_THRESHOLD, bt=BREAK_THRESHOLD, mp=MIN_PAIRS)


def load_data():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(PAIRS_SQL, conn)
    conn.close()
    df["mid_pct"] = 100.0 - df["era_continuity_pct"] - df["era_break_pct"]
    return df


# ---------------------------------------------------------------------------
# Chart 1: Mean absolute year gap (ranked bar)
# ---------------------------------------------------------------------------
def chart_mean_gap(df):
    fig = go.Figure(go.Bar(
        x=df["mean_abs_gap"],
        y=df["station_show"],
        orientation="h",
        marker=dict(
            color=df["mean_abs_gap"],
            colorscale="RdYlGn_r",
            showscale=True,
            colorbar=dict(title="Avg year gap"),
        ),
        text=df["mean_abs_gap"].apply(lambda v: f"{v:.1f} yrs"),
        textposition="outside",
        customdata=df[["era_continuity_pct", "era_break_pct", "total_pairs"]].values,
        hovertemplate=(
            f"<b>%{{y}}</b><br>"
            f"Mean gap: %{{x:.1f}} yrs<br>"
            f"Era-continuous (<={CONTINUITY_THRESHOLD}yr): %{{customdata[0]:.1f}}%<br>"
            f"Era breaks (>{BREAK_THRESHOLD}yr): %{{customdata[1]:.1f}}%<br>"
            f"Total pairs: %{{customdata[2]}}<extra></extra>"
        ),
    ))
    fig.update_layout(
        title=dict(text="Mean Absolute Year Gap Between Consecutive Tracks -- by Show",
                   font=dict(size=16)),
        xaxis_title="Mean absolute album year gap",
        yaxis=dict(categoryorder="total ascending"),
        height=max(400, len(df) * 55),
        margin=dict(l=200, r=120, t=60, b=60),
        plot_bgcolor="white",
        paper_bgcolor="white",
    )
    fig.update_xaxes(showgrid=True, gridcolor="#eeeeee")
    path = OUTPUT_DIR / "era_continuity_mean_gap.html"
    fig.write_html(str(path))
    print(f"Saved: {path}")


# ---------------------------------------------------------------------------
# Chart 2: Fingerprint scatter -- mean gap vs era break pct
# ---------------------------------------------------------------------------
def chart_fingerprint(df):
    # Bubble size: scale total_pairs to visible range
    size_ref = max(df["total_pairs"]) / 800

    fig = go.Figure(go.Scatter(
        x=df["mean_abs_gap"],
        y=df["era_break_pct"],
        mode="markers+text",
        text=df["station_show"],
        textposition="top center",
        marker=dict(
            size=df["total_pairs"],
            sizemode="area",
            sizeref=size_ref,
            sizemin=8,
            color=df["avg_era"],
            colorscale="Viridis",
            showscale=True,
            colorbar=dict(title="Avg era (yr)"),
            line=dict(width=1, color="white"),
        ),
        customdata=df[["era_continuity_pct", "total_pairs", "avg_era"]].values,
        hovertemplate=(
            f"<b>%{{text}}</b><br>"
            f"Mean gap: %{{x:.1f}} yrs<br>"
            f"Era breaks (>{BREAK_THRESHOLD}yr): %{{y:.1f}}%<br>"
            f"Era-continuous (<={CONTINUITY_THRESHOLD}yr): %{{customdata[0]:.1f}}%<br>"
            f"Total pairs: %{{customdata[1]}}<br>"
            f"Avg era: %{{customdata[2]:.0f}}<extra></extra>"
        ),
    ))

    # Quadrant annotation lines (median-ish guides)
    fig.add_vline(x=15, line_dash="dash", line_color="#cccccc", line_width=1)
    fig.add_hline(y=50, line_dash="dash", line_color="#cccccc", line_width=1)

    fig.update_layout(
        title=dict(
            text="Programming Style Fingerprint -- Mean Gap vs Era Break Rate",
            font=dict(size=16),
        ),
        xaxis_title="Mean absolute year gap",
        yaxis_title=f"Era break rate (% pairs > {BREAK_THRESHOLD} yr gap)",
        height=600,
        plot_bgcolor="white",
        paper_bgcolor="white",
    )
    fig.update_xaxes(showgrid=True, gridcolor="#eeeeee")
    fig.update_yaxes(showgrid=True, gridcolor="#eeeeee")
    path = OUTPUT_DIR / "era_continuity_fingerprint.html"
    fig.write_html(str(path))
    print(f"Saved: {path}")


# ---------------------------------------------------------------------------
# Chart 3: Stacked bar -- tight / mid / break bucket breakdown
# ---------------------------------------------------------------------------
def chart_buckets(df):
    df_sorted = df.sort_values("era_continuity_pct", ascending=True)

    fig = go.Figure()
    fig.add_trace(go.Bar(
        name=f"Tight (<= {CONTINUITY_THRESHOLD} yr)",
        x=df_sorted["era_continuity_pct"],
        y=df_sorted["station_show"],
        orientation="h",
        marker_color="#2ecc71",
        hovertemplate="<b>%{y}</b><br>Tight: %{x:.1f}%<extra></extra>",
    ))
    fig.add_trace(go.Bar(
        name=f"Mid ({CONTINUITY_THRESHOLD + 1}-{BREAK_THRESHOLD} yr)",
        x=df_sorted["mid_pct"],
        y=df_sorted["station_show"],
        orientation="h",
        marker_color="#f39c12",
        hovertemplate="<b>%{y}</b><br>Mid: %{x:.1f}%<extra></extra>",
    ))
    fig.add_trace(go.Bar(
        name=f"Era break (> {BREAK_THRESHOLD} yr)",
        x=df_sorted["era_break_pct"],
        y=df_sorted["station_show"],
        orientation="h",
        marker_color="#e74c3c",
        hovertemplate="<b>%{y}</b><br>Break: %{x:.1f}%<extra></extra>",
    ))

    fig.update_layout(
        barmode="stack",
        title=dict(text="Consecutive-Pair Year Gap Breakdown by Show", font=dict(size=16)),
        xaxis_title="% of consecutive pairs",
        xaxis=dict(range=[0, 100]),
        height=max(400, len(df) * 55),
        margin=dict(l=200, r=40, t=60, b=60),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        plot_bgcolor="white",
        paper_bgcolor="white",
    )
    fig.update_xaxes(showgrid=True, gridcolor="#eeeeee")
    path = OUTPUT_DIR / "era_continuity_buckets.html"
    fig.write_html(str(path))
    print(f"Saved: {path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def run_era_continuity():
    print("=== Era Continuity Analysis ===")
    print(f"  Continuity threshold : <= {CONTINUITY_THRESHOLD} yr gap")
    print(f"  Era break threshold  : >  {BREAK_THRESHOLD} yr gap")
    print(f"  Min pairs to include : {MIN_PAIRS}")
    print()

    df = load_data()

    print(f"  Shows included: {len(df)}")
    print(f"  Total pairs analysed: {df['total_pairs'].sum():,}")
    print()

    # Print summary table to terminal
    header = f"  {'Show':<40} {'Pairs':>6} {'MeanGap':>8} {'Cont%':>7} {'Break%':>7} {'AvgEra':>7}"
    print(header)
    print("  " + "-" * 80)
    for _, row in df.iterrows():
        print(
            f"  {row['station_show']:<40} {int(row['total_pairs']):>6} "
            f"{row['mean_abs_gap']:>8.1f} {row['era_continuity_pct']:>6.1f}% "
            f"{row['era_break_pct']:>6.1f}% {int(row['avg_era']):>7}"
        )

    print()

    # Save CSV
    csv_path = OUTPUT_DIR / "era_continuity.csv"
    df.to_csv(csv_path, index=False, encoding="utf-8")
    print(f"Saved: {csv_path}")
    print()

    chart_mean_gap(df)
    chart_fingerprint(df)
    chart_buckets(df)

    print()
    print("=== Done ===")
    return df


if __name__ == "__main__":
    run_era_continuity()
