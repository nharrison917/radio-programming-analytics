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

# ---------------------------------------------------------------------------
# Segmentation parameters
# SEGMENT_PARAMS maps show name -> (band, min_inband, consecutive_oob).
# Shows not listed fall back to "default".  All current shows share the same
# parameters; add a show-specific entry here if future shows need different
# tuning (e.g. a wider band for a decade-format show).
# ---------------------------------------------------------------------------
SEGMENT_SHOWS = (
    "10 @ 10",
    "10 @ 10 Weekend Replay",
    "This Just In with Meg White",
)
SEGMENT_PARAMS = {
    # (band_yr, min_inband_tracks, consecutive_oob_to_terminate)
    "default": (3, 8, 2),
}


def _show_params(show):
    """Return (band, min_inband, consec_oob) for the given show."""
    return SEGMENT_PARAMS.get(show, SEGMENT_PARAMS["default"])
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
    WHERE ct.spotify_status = 'SUCCESS'
      AND ct.mb_lookup_status IS NOT NULL
      AND ct.mb_ta_status IS NOT NULL
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
        annotations=[dict(
            text="* = density-segmented pairs",
            xref="paper", yref="paper",
            x=0.0, y=-0.06,
            showarrow=False,
            font=dict(size=11, color="#666666"),
            xanchor="left",
        )],
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
        annotations=[dict(
            text="* = density-segmented pairs",
            xref="paper", yref="paper",
            x=0.0, y=-0.06,
            showarrow=False,
            font=dict(size=11, color="#666666"),
            xanchor="left",
        )],
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
        annotations=[dict(
            text="* = density-segmented pairs",
            xref="paper", yref="paper",
            x=0.0, y=-0.06,
            showarrow=False,
            font=dict(size=11, color="#666666"),
            xanchor="left",
        )],
    )
    fig.update_xaxes(showgrid=True, gridcolor="#eeeeee")
    path = OUTPUT_DIR / "era_continuity_buckets.html"
    fig.write_html(str(path))
    print(f"Saved: {path}")


# ---------------------------------------------------------------------------
# 10@10 density-based segmentation
# ---------------------------------------------------------------------------

_TRACKS_SQL_TEMPLATE = """
SELECT
    p.id                       AS play_id,
    p.play_ts,
    p.station_show,
    DATE(p.play_ts)            AS play_date,
    STRFTIME('%H', p.play_ts)  AS play_hour,
    ct.canonical_id,
    ct.norm_artist,
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
    END AS best_year
FROM plays p
JOIN plays_to_canonical ptc ON p.id = ptc.play_id
JOIN canonical_tracks ct ON ptc.canonical_id = ct.canonical_id
WHERE p.station_show IN ({placeholders})
  AND ct.spotify_status = 'SUCCESS'
  AND ct.mb_lookup_status IS NOT NULL
  AND ct.mb_ta_status IS NOT NULL
ORDER BY p.play_ts
"""


def _modal_era(years, band):
    """Find Y in years that maximises count of tracks with |year - Y| <= band."""
    candidates = [y for y in years if y is not None]
    if not candidates:
        return None
    best_y, best_count = None, 0
    for candidate in candidates:
        count = sum(1 for y in candidates if abs(y - candidate) <= band)
        if count > best_count:
            best_count, best_y = count, candidate
    return best_y


def _segment_block(years, band, min_inband, consec_oob):
    """
    Density-based era segmentation for a single hour block.

    Returns the ordered list of in-band year values that form the confirmed
    segment, or None if the block does not accumulate min_inband in-band
    tracks before consec_oob consecutive out-of-band tracks terminate it.
    """
    modal = _modal_era(years, band)
    if modal is None:
        return None

    in_band = []
    consecutive_oob = 0
    for y in years:
        if y is not None and abs(y - modal) <= band:
            in_band.append(y)
            consecutive_oob = 0
        else:
            consecutive_oob += 1
            if consecutive_oob >= consec_oob:
                break

    return in_band if len(in_band) >= min_inband else None


def get_inband_tracks(tracks_df):
    """
    Apply density-based segmentation per (station_show, play_date, play_hour) block.
    Returns a DataFrame containing only in-band tracks from valid segments.

    tracks_df must have columns: play_ts, station_show, play_date, play_hour, best_year
    Any additional columns (play_id, canonical_id, norm_artist) are preserved.
    Per-show parameters are looked up from SEGMENT_PARAMS via _show_params().
    """
    keep_indices = []

    for (show, date, hour), grp in tracks_df.groupby(
        ["station_show", "play_date", "play_hour"]
    ):
        band, min_inband, consec_oob = _show_params(show)
        grp_sorted = grp.sort_values("play_ts")
        years = grp_sorted["best_year"].tolist()
        modal = _modal_era(years, band)
        if modal is None:
            continue

        in_band_indices = []
        consecutive_oob = 0
        for idx, y in zip(grp_sorted.index, years):
            if y is not None and abs(y - modal) <= band:
                in_band_indices.append(idx)
                consecutive_oob = 0
            else:
                consecutive_oob += 1
                if consecutive_oob >= consec_oob:
                    break

        if len(in_band_indices) >= min_inband:
            keep_indices.extend(in_band_indices)

    return tracks_df.loc[keep_indices].copy()


def load_segmented_tracks():
    """Load all plays for SEGMENT_SHOWS with best_year and identity columns."""
    placeholders = ",".join("?" * len(SEGMENT_SHOWS))
    sql = _TRACKS_SQL_TEMPLATE.format(placeholders=placeholders)
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(sql, conn, params=list(SEGMENT_SHOWS))
    conn.close()
    df["play_ts"] = pd.to_datetime(df["play_ts"], errors="coerce")
    return df


def compute_segmented_metrics(tracks_df):
    """
    Apply density-based segmentation to each (station_show, play_date, play_hour)
    block.  Returns:
      - metrics_df  : show-level aggregated pair metrics from in-band tracks only
      - block_stats : per-block summary (total_tracks, in_band, segment_valid)
    """
    all_pairs = []
    block_rows = []

    for (show, date, hour), grp in tracks_df.groupby(
        ["station_show", "play_date", "play_hour"]
    ):
        band, min_inband, consec_oob = _show_params(show)
        years = grp.sort_values("play_ts")["best_year"].tolist()
        in_band = _segment_block(years, band, min_inband, consec_oob)

        block_rows.append({
            "station_show": show,
            "play_date": date,
            "play_hour": hour,
            "total_tracks": len(years),
            "in_band_tracks": len(in_band) if in_band else 0,
            "segment_valid": in_band is not None,
        })

        if in_band and len(in_band) >= 2:
            for y1, y2 in zip(in_band, in_band[1:]):
                all_pairs.append({
                    "station_show": show,
                    "gap": abs(y2 - y1),
                    "mid_era": (y1 + y2) / 2.0,
                })

    block_stats = pd.DataFrame(block_rows)

    if not all_pairs:
        return pd.DataFrame(), block_stats

    pairs_df = pd.DataFrame(all_pairs)

    metrics_rows = []
    for show, grp in pairs_df.groupby("station_show"):
        gaps = grp["gap"]
        era_cont_pct  = round(100.0 * (gaps <= CONTINUITY_THRESHOLD).sum() / len(gaps), 1)
        era_break_pct = round(100.0 * (gaps > BREAK_THRESHOLD).sum() / len(gaps), 1)
        metrics_rows.append({
            "station_show":       show,
            "total_pairs":        len(gaps),
            "mean_abs_gap":       round(gaps.mean(), 2),
            "era_continuity_pct": era_cont_pct,
            "era_break_pct":      era_break_pct,
            "mid_pct":            round(100.0 - era_cont_pct - era_break_pct, 1),
            "avg_era":            round(grp["mid_era"].mean(), 0),
        })

    return pd.DataFrame(metrics_rows), block_stats


def print_segmented_comparison(baseline_df, seg_df, block_stats):
    """Print side-by-side comparison of unfiltered vs segment-filtered metrics."""
    default_band, default_min, default_consec = SEGMENT_PARAMS["default"]
    print()
    print("=== Segment-Filtered Analysis ===")
    print(
        f"  Default params -- Band: +/-{default_band} yr  |  "
        f"Min in-band: {default_min} tracks  |  "
        f"Break rule: {default_consec} consecutive OOB"
    )
    print()

    total_blocks = len(block_stats)
    valid_blocks = block_stats["segment_valid"].sum()
    print(f"  Block summary ({total_blocks} total hour blocks):")
    for show in SEGMENT_SHOWS:
        sub = block_stats[block_stats["station_show"] == show]
        v = sub["segment_valid"].sum()
        t = len(sub)
        print(f"    {show}: {v}/{t} valid segments ({100*v//t}%)")
    print()

    header = (
        f"  {'Show':<30} {'Metric':<12} "
        f"{'Unfiltered':>12} {'Segmented':>12} {'Delta':>8}"
    )
    print(header)
    print("  " + "-" * 78)

    for show in SEGMENT_SHOWS:
        base_row = baseline_df[baseline_df["station_show"] == show]
        seg_row = seg_df[seg_df["station_show"] == show]
        if base_row.empty or seg_row.empty:
            continue
        b = base_row.iloc[0]
        s = seg_row.iloc[0]

        metrics = [
            ("Pairs",     int(b["total_pairs"]),        int(s["total_pairs"]),        None,  "d"),
            ("Mean gap",  b["mean_abs_gap"],             s["mean_abs_gap"],            True,  ".1f"),
            ("Cont%",     b["era_continuity_pct"],       s["era_continuity_pct"],      False, ".1f"),
            ("Break%",    b["era_break_pct"],            s["era_break_pct"],           True,  ".1f"),
        ]
        first = True
        for label, base_val, seg_val, lower_is_better, fmt in metrics:
            show_label = show if first else ""
            first = False
            if fmt == "d":
                delta_str = f"{seg_val - base_val:+d}"
                base_str  = str(base_val)
                seg_str   = str(seg_val)
            else:
                delta = seg_val - base_val
                delta_str = f"{delta:+.1f}"
                base_str  = format(base_val, fmt)
                seg_str   = format(seg_val, fmt)
            print(
                f"  {show_label:<30} {label:<12} "
                f"{base_str:>12} {seg_str:>12} {delta_str:>8}"
            )
        print()


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

    # Save CSV (unmodified -- no asterisks)
    csv_path = OUTPUT_DIR / "era_continuity.csv"
    df.to_csv(csv_path, index=False, encoding="utf-8")
    print(f"Saved: {csv_path}")
    print()

    # --- segmented shows ---
    tracks_seg = load_segmented_tracks()
    seg_metrics, block_stats = compute_segmented_metrics(tracks_seg)

    # Build display_df: replace SEGMENT_SHOWS rows with segmented values + asterisk labels
    display_df = df.copy()
    if not seg_metrics.empty:
        for _, seg_row in seg_metrics.iterrows():
            show = seg_row["station_show"]
            mask = display_df["station_show"] == show
            if not mask.any():
                continue
            for col in ["total_pairs", "mean_abs_gap", "era_continuity_pct",
                        "era_break_pct", "mid_pct", "avg_era"]:
                if col in seg_row.index:
                    display_df.loc[mask, col] = seg_row[col]
            display_df.loc[mask, "station_show"] = show + " *"

    chart_mean_gap(display_df)
    chart_fingerprint(display_df)
    chart_buckets(display_df)

    # Baseline for segmented shows (subset of raw df -- no asterisks)
    baseline_seg = df[df["station_show"].isin(SEGMENT_SHOWS)].reset_index(drop=True)

    if not seg_metrics.empty:
        print_segmented_comparison(baseline_seg, seg_metrics, block_stats)

        seg_csv = OUTPUT_DIR / "era_continuity_segmented.csv"
        seg_metrics.to_csv(seg_csv, index=False)
        print(f"Saved: {seg_csv}")

    print()
    print("=== Done ===")
    return df


if __name__ == "__main__":
    run_era_continuity()
