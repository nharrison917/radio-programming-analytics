# analytics/show_clustering.py
# -*- coding: utf-8 -*-
"""
Show-level clustering analysis.

Three passes:
  1. Scalar features only  (rotation depth, era, exclusivity, era-mixing)
  2. Repertoire only       (binary cosine similarity: top-10 artists + top-20 tracks)
  3. Combined              (scalar features + MDS coordinates from repertoire)

Outputs
-------
analytics/outputs/cluster_scalar_dendrogram.html
analytics/outputs/cluster_scalar_heatmap.html
analytics/outputs/cluster_repertoire_dendrogram.html
analytics/outputs/cluster_combined_dendrogram.html
analytics/outputs/show_clustering_features.csv
"""

import sys
import warnings
import sqlite3
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

import plotly.graph_objects as go
import plotly.figure_factory as ff
from scipy.cluster.hierarchy import linkage, fcluster
from scipy.spatial.distance import squareform
from sklearn.preprocessing import StandardScaler
from sklearn.manifold import MDS

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

DB_PATH = Path(__file__).resolve().parents[1] / "radio_plays.db"
OUTPUT_DIR = Path(__file__).resolve().parent / "outputs"
OUTPUT_DIR.mkdir(exist_ok=True)

REPERTOIRE_DAYS = 60
TOP_ARTISTS = 10
TOP_TRACKS = 20
ERA_CONTINUITY_THRESHOLD = 3
ERA_BREAK_THRESHOLD = 10
FRESHNESS_YEARS = 5


# ---------------------------------------------------------------------------
# Data loading helpers
# ---------------------------------------------------------------------------

def _get_conn():
    return sqlite3.connect(DB_PATH)


def _load_plays():
    """Base play-level dataset joined to canonical tracks."""
    q = """
    SELECT
        p.id AS play_id,
        p.play_ts,
        p.station_show,
        c.canonical_id,
        c.norm_artist,
        CASE
            WHEN c.mb_first_release_year IS NOT NULL
             AND c.mb_first_release_year < c.spotify_album_release_year
            THEN c.mb_first_release_year
            ELSE c.spotify_album_release_year
        END AS best_year
    FROM plays p
    JOIN plays_to_canonical pc ON p.id = pc.play_id
    JOIN canonical_tracks c ON pc.canonical_id = c.canonical_id
    WHERE p.is_music_show = 1
    """
    conn = _get_conn()
    df = pd.read_sql_query(q, conn)
    conn.close()
    df["play_ts"] = pd.to_datetime(df["play_ts"], errors="coerce")
    return df


# ---------------------------------------------------------------------------
# Scalar feature computation
# ---------------------------------------------------------------------------

def compute_scalar_features(df):
    """
    Returns a DataFrame with one row per show and columns:
      artist_entropy, unique_artists_per_hour, avg_best_year,
      freshness_pct, exclusive_artist_pct, era_continuity_mean_gap
    """
    shows = sorted(df["station_show"].unique())

    # -- artist_entropy --
    def _entropy(series):
        probs = series.value_counts(normalize=True)
        return float(-np.sum(probs * np.log2(probs + 1e-12)))

    entropy = (
        df.groupby("station_show")["norm_artist"]
        .apply(_entropy)
        .rename("artist_entropy")
    )

    # -- unique_artists_per_hour --
    df_hr = df.copy()
    df_hr["play_hour"] = df_hr["play_ts"].dt.floor("h")
    broadcast_hours = (
        df_hr.groupby("station_show")["play_hour"].nunique()
    )
    unique_artists = df.groupby("station_show")["norm_artist"].nunique()
    uaph = (unique_artists / broadcast_hours).rename("unique_artists_per_hour")

    # -- avg_best_year --
    avg_year = (
        df.dropna(subset=["best_year"])
        .groupby("station_show")["best_year"]
        .mean()
        .rename("avg_best_year")
    )

    # -- freshness_pct --
    current_year = datetime.now().year
    cutoff_year = current_year - FRESHNESS_YEARS
    df_fresh = df[df["best_year"] >= cutoff_year]
    total_ct = df.groupby("station_show").size()
    fresh_ct = df_fresh.groupby("station_show").size().reindex(total_ct.index, fill_value=0)
    freshness = (fresh_ct / total_ct).rename("freshness_pct")

    # -- exclusive_artist_pct --
    artist_show_ct = (
        df.groupby("norm_artist")["station_show"].nunique()
    )
    exclusive_artists = artist_show_ct[artist_show_ct == 1].index
    df_excl = df[df["norm_artist"].isin(exclusive_artists)]
    excl_ct = df_excl.groupby("station_show")["norm_artist"].nunique()
    total_artist_ct = df.groupby("station_show")["norm_artist"].nunique()
    excl_pct = (
        excl_ct.reindex(total_artist_ct.index, fill_value=0) / total_artist_ct
    ).rename("exclusive_artist_pct")

    # -- era_continuity_mean_gap --
    era_sql = """
    WITH ranked AS (
        SELECT
            p.play_ts,
            p.station_show,
            DATE(p.play_ts) AS play_date,
            CASE
                WHEN ct.mb_first_release_year IS NOT NULL
                 AND ct.mb_first_release_year < ct.spotify_album_release_year
                THEN ct.mb_first_release_year
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
            ABS(r2.yr - r1.yr) AS gap
        FROM ranked r1
        JOIN ranked r2
            ON  r1.station_show = r2.station_show
            AND r1.play_date    = r2.play_date
            AND r2.rn           = r1.rn + 1
    )
    SELECT station_show, AVG(gap) AS mean_abs_gap
    FROM pairs
    GROUP BY station_show
    HAVING COUNT(*) >= 20
    """
    conn = _get_conn()
    era_df = pd.read_sql_query(era_sql, conn)
    conn.close()
    era_gap = era_df.set_index("station_show")["mean_abs_gap"].rename("era_continuity_mean_gap")

    # -- Assemble --
    features = pd.DataFrame({
        "artist_entropy": entropy,
        "unique_artists_per_hour": uaph,
        "avg_best_year": avg_year,
        "freshness_pct": freshness,
        "exclusive_artist_pct": excl_pct,
        "era_continuity_mean_gap": era_gap,
    })
    features.index.name = "station_show"
    features = features.loc[features.index.isin(shows)]
    return features.dropna()


# ---------------------------------------------------------------------------
# Repertoire similarity
# ---------------------------------------------------------------------------

def compute_repertoire_similarity(days=REPERTOIRE_DAYS):
    """
    Binary cosine similarity: top-N artists + top-M tracks per show
    over the last `days` days.

    Returns (shows list, similarity DataFrame).
    """
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    conn = _get_conn()
    artists_df = pd.read_sql_query("""
        SELECT p.station_show, c.norm_artist, COUNT(*) AS plays
        FROM plays p
        JOIN plays_to_canonical pc ON p.id = pc.play_id
        JOIN canonical_tracks c ON pc.canonical_id = c.canonical_id
        WHERE p.is_music_show = 1 AND p.play_ts >= ?
        GROUP BY p.station_show, c.norm_artist
    """, conn, params=[cutoff])

    tracks_df = pd.read_sql_query("""
        SELECT p.station_show, c.canonical_id, COUNT(*) AS plays
        FROM plays p
        JOIN plays_to_canonical pc ON p.id = pc.play_id
        JOIN canonical_tracks c ON pc.canonical_id = c.canonical_id
        WHERE p.is_music_show = 1 AND p.play_ts >= ?
        GROUP BY p.station_show, c.canonical_id
    """, conn, params=[cutoff])
    conn.close()

    shows = sorted(artists_df["station_show"].unique())

    top_artists = {}
    top_tracks = {}
    for show in shows:
        ag = artists_df[artists_df["station_show"] == show]
        tg = tracks_df[tracks_df["station_show"] == show]
        top_artists[show] = set(ag.nlargest(TOP_ARTISTS, "plays")["norm_artist"])
        top_tracks[show] = set(tg.nlargest(TOP_TRACKS, "plays")["canonical_id"])

    all_artists = sorted(set().union(*top_artists.values()))
    all_tracks = sorted(set().union(*top_tracks.values()))
    vocab = [("a", x) for x in all_artists] + [("t", x) for x in all_tracks]

    mat = np.zeros((len(shows), len(vocab)), dtype=float)
    for i, show in enumerate(shows):
        for j, (kind, item) in enumerate(vocab):
            if kind == "a" and item in top_artists[show]:
                mat[i, j] = 1.0
            elif kind == "t" and item in top_tracks[show]:
                mat[i, j] = 1.0

    norms = np.linalg.norm(mat, axis=1, keepdims=True)
    mat_norm = mat / np.where(norms == 0, 1, norms)
    sim = mat_norm @ mat_norm.T

    return shows, pd.DataFrame(sim, index=shows, columns=shows)


# ---------------------------------------------------------------------------
# Visualisation helpers
# ---------------------------------------------------------------------------

CLUSTER_COLORS = ["#2196F3", "#FF5722", "#4CAF50", "#9C27B0", "#FF9800"]


def _dendrogram(dist_condensed, labels, title, out_path, k_hint=3):
    """
    Plotly figure_factory dendrogram from a condensed distance vector.
    Colours cut at k_hint clusters.
    """
    Z = linkage(dist_condensed, method="ward")
    cluster_ids = fcluster(Z, t=k_hint, criterion="maxclust")

    label_colors = {
        label: CLUSTER_COLORS[(cluster_ids[i] - 1) % len(CLUSTER_COLORS)]
        for i, label in enumerate(labels)
    }

    fig = ff.create_dendrogram(
        np.zeros((len(labels), len(labels))),  # placeholder -- we supply Z via dist
        labels=labels,
        linkagefun=lambda x: Z,
        color_threshold=0,
    )

    # Override leaf label colours
    for trace in fig.data:
        if trace.mode == "text" if hasattr(trace, "mode") else False:
            trace.textfont = dict(color=[label_colors.get(t, "#333") for t in trace.text])

    fig.update_layout(
        title=dict(text=title, font=dict(size=15)),
        plot_bgcolor="white",
        paper_bgcolor="white",
        height=500,
        margin=dict(l=40, r=40, t=60, b=120),
    )
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(title="Ward linkage distance", showgrid=True, gridcolor="#eeeeee")
    fig.write_html(str(out_path))
    print(f"Saved: {out_path}")


def _scalar_heatmap(features_scaled, features_raw, shows, out_path):
    """
    Heatmap of z-scored scalar features. Raw values in hover.
    """
    feature_names = list(features_raw.columns)
    z = features_scaled.T.tolist()

    hover = []
    for fi, fname in enumerate(feature_names):
        row = []
        for show in shows:
            raw_val = features_raw.loc[show, fname]
            row.append(f"{show}<br>{fname}: {raw_val:.3f}")
        hover.append(row)

    fig = go.Figure(go.Heatmap(
        z=z,
        x=shows,
        y=feature_names,
        colorscale="RdBu",
        zmid=0,
        text=hover,
        hoverinfo="text",
        colorbar=dict(title="z-score"),
    ))
    fig.update_layout(
        title=dict(text="Scalar Feature Heatmap (z-scored)", font=dict(size=15)),
        plot_bgcolor="white",
        paper_bgcolor="white",
        height=420,
        margin=dict(l=200, r=40, t=60, b=120),
        xaxis=dict(tickangle=-35),
    )
    fig.write_html(str(out_path))
    print(f"Saved: {out_path}")


def _similarity_heatmap(sim_df, out_path):
    shows = list(sim_df.index)
    fig = go.Figure(go.Heatmap(
        z=sim_df.values.tolist(),
        x=shows,
        y=shows,
        colorscale="Blues",
        zmin=0, zmax=1,
        text=[[f"{sim_df.index[i]} vs {sim_df.columns[j]}<br>sim: {sim_df.values[i,j]:.2f}"
               for j in range(len(shows))] for i in range(len(shows))],
        hoverinfo="text",
        colorbar=dict(title="Cosine sim"),
    ))
    fig.update_layout(
        title=dict(text="Repertoire Cosine Similarity (top-10 artists + top-20 tracks, last 60 days)",
                   font=dict(size=14)),
        plot_bgcolor="white",
        paper_bgcolor="white",
        height=520,
        margin=dict(l=180, r=40, t=60, b=160),
        xaxis=dict(tickangle=-40),
    )
    fig.write_html(str(out_path))
    print(f"Saved: {out_path}")


# ---------------------------------------------------------------------------
# Main clustering passes
# ---------------------------------------------------------------------------

def run_show_clustering():
    print("=== Show Clustering Analysis ===")
    print()

    # -- Load plays once --
    df = _load_plays()

    # -----------------------------------------------------------------------
    # PASS 1: Scalar features
    # -----------------------------------------------------------------------
    print("--- Pass 1: Scalar Features ---")
    scalar_df = compute_scalar_features(df)
    shows_scalar = list(scalar_df.index)
    print(f"  Shows: {len(shows_scalar)}")
    print(f"  Features: {list(scalar_df.columns)}")
    print()

    print(f"{'Show':<40} " + "  ".join(f"{c[:8]:>8}" for c in scalar_df.columns))
    print("-" * 100)
    for show, row in scalar_df.iterrows():
        vals = "  ".join(f"{v:>8.3f}" for v in row)
        print(f"{show:<40} {vals}")
    print()

    scaler = StandardScaler()
    scalar_scaled = scaler.fit_transform(scalar_df.values)

    dist_scalar = np.sqrt(
        np.maximum(
            np.sum((scalar_scaled[:, None, :] - scalar_scaled[None, :, :]) ** 2, axis=-1),
            0
        )
    )
    dist_cond_scalar = squareform(dist_scalar, checks=False)

    _dendrogram(
        dist_cond_scalar, shows_scalar,
        "Show Clustering -- Scalar Features (Ward linkage)",
        OUTPUT_DIR / "cluster_scalar_dendrogram.html",
        k_hint=3,
    )
    _scalar_heatmap(
        scalar_scaled, scalar_df, shows_scalar,
        OUTPUT_DIR / "cluster_scalar_heatmap.html",
    )

    # -----------------------------------------------------------------------
    # PASS 2: Repertoire similarity
    # -----------------------------------------------------------------------
    print()
    print("--- Pass 2: Repertoire Similarity ---")
    shows_rep, sim_df = compute_repertoire_similarity(days=REPERTOIRE_DAYS)
    print(f"  Shows: {len(shows_rep)}")
    print(f"  Vocab: top-{TOP_ARTISTS} artists + top-{TOP_TRACKS} tracks, last {REPERTOIRE_DAYS} days")
    print()

    print("  Similarity matrix (excerpt -- top pairs):")
    sim_arr = sim_df.values
    pairs = []
    for i in range(len(shows_rep)):
        for j in range(i + 1, len(shows_rep)):
            pairs.append((sim_arr[i, j], shows_rep[i], shows_rep[j]))
    pairs.sort(reverse=True)
    for score, a, b in pairs[:5]:
        print(f"    {score:.3f}  {a}  <->  {b}")
    print()

    dist_rep = 1.0 - sim_arr
    np.fill_diagonal(dist_rep, 0.0)
    dist_cond_rep = squareform(dist_rep, checks=False)

    _dendrogram(
        dist_cond_rep, shows_rep,
        f"Show Clustering -- Repertoire (top-{TOP_ARTISTS} artists + top-{TOP_TRACKS} tracks, last {REPERTOIRE_DAYS} days)",
        OUTPUT_DIR / "cluster_repertoire_dendrogram.html",
        k_hint=3,
    )
    _similarity_heatmap(sim_df, OUTPUT_DIR / "cluster_repertoire_heatmap.html")

    # -----------------------------------------------------------------------
    # PASS 3: Combined (scalars + MDS coords from repertoire)
    # -----------------------------------------------------------------------
    print()
    print("--- Pass 3: Combined ---")

    # Align shows present in both analyses
    common_shows = sorted(set(shows_scalar) & set(shows_rep))
    print(f"  Shows in both analyses: {len(common_shows)}")
    if len(common_shows) < len(shows_scalar):
        dropped = set(shows_scalar) - set(common_shows)
        print(f"  Dropped from scalar set (missing in repertoire): {dropped}")

    # MDS from repertoire distance matrix -- 2 dimensions
    sim_common = sim_df.loc[common_shows, common_shows].values
    dist_common = 1.0 - sim_common
    np.fill_diagonal(dist_common, 0.0)

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", FutureWarning)
        mds = MDS(n_components=2, dissimilarity="precomputed", random_state=42, n_init=1)
        mds_coords = mds.fit_transform(dist_common)
    mds_df = pd.DataFrame(
        mds_coords, index=common_shows, columns=["repertoire_mds1", "repertoire_mds2"]
    )

    # Align scalar features to common shows
    scalar_common = scalar_df.loc[common_shows]
    combined_raw = pd.concat([scalar_common, mds_df], axis=1)

    combined_scaled = StandardScaler().fit_transform(combined_raw.values)

    print(f"  Combined features ({combined_raw.shape[1]}): {list(combined_raw.columns)}")
    print()

    dist_combined = np.sqrt(
        np.maximum(
            np.sum((combined_scaled[:, None, :] - combined_scaled[None, :, :]) ** 2, axis=-1),
            0
        )
    )
    dist_cond_combined = squareform(dist_combined, checks=False)

    _dendrogram(
        dist_cond_combined, common_shows,
        "Show Clustering -- Combined (scalar + repertoire MDS, unweighted)",
        OUTPUT_DIR / "cluster_combined_dendrogram.html",
        k_hint=3,
    )

    # -----------------------------------------------------------------------
    # PASS 4: Combined -- equal-weight (MDS scaled up to match scalar vote count)
    # -----------------------------------------------------------------------
    print()
    print("--- Pass 4: Combined Equal-Weight ---")
    print("  Scalar features: 6 dimensions")
    print("  Repertoire MDS: 2 dimensions x3 weight = 6 effective votes")
    print()

    n_scalar = scalar_common.shape[1]        # 6
    n_mds = mds_df.shape[1]                  # 2
    rep_weight = n_scalar / n_mds            # = 3.0

    combined_eq_raw = pd.concat([scalar_common, mds_df * rep_weight], axis=1)
    combined_eq_scaled = StandardScaler().fit_transform(combined_eq_raw.values)

    dist_eq = np.sqrt(
        np.maximum(
            np.sum((combined_eq_scaled[:, None, :] - combined_eq_scaled[None, :, :]) ** 2, axis=-1),
            0
        )
    )
    dist_cond_eq = squareform(dist_eq, checks=False)

    _dendrogram(
        dist_cond_eq, common_shows,
        "Show Clustering -- Combined Equal-Weight (scalar 6v : repertoire 6v)",
        OUTPUT_DIR / "cluster_combined_equalweight_dendrogram.html",
        k_hint=3,
    )

    # -----------------------------------------------------------------------
    # Export features CSV
    # -----------------------------------------------------------------------
    combined_raw.to_csv(OUTPUT_DIR / "show_clustering_features.csv", encoding="utf-8")
    print()
    print(f"Saved: {OUTPUT_DIR / 'show_clustering_features.csv'}")
    print()
    print("=== Done ===")


if __name__ == "__main__":
    run_show_clustering()
