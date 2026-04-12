# -*- coding: utf-8 -*-
"""
Segment Breakers Report

Identifies canonical tracks that appear out of place in era-specific shows:
  - "90's at Night": tracks with best_year outside 1989-2000 (inclusive), or null
  - SEGMENT_SHOWS (10 @ 10, Weekend Replay, This Just In):
    out-of-band tracks from valid density-segmented blocks

"Out-of-band" uses the same density segmentation as era_continuity.py.
Blocks that fail to reach min_inband in-band tracks are excluded entirely --
only valid segments contribute OOB rows.

Output
------
analytics/outputs/segment_breakers.csv
"""

import sqlite3
import pandas as pd
from pathlib import Path

from analytics.era_continuity import SEGMENT_SHOWS, _show_params, _modal_era

DB_PATH = Path(__file__).resolve().parents[1] / "radio_plays.db"
OUTPUT_DIR = Path(__file__).resolve().parent / "outputs"
OUTPUT_DIR.mkdir(exist_ok=True)

NINETIES_SHOW = "90's at Night"
YEAR_LOW  = 1989   # inclusive lower bound (90s +/- 1)
YEAR_HIGH = 2000   # inclusive upper bound

# ---------------------------------------------------------------------------
# Shared best_year CASE expression (mirrors era_continuity.py and analysis.py)
# No spotify_status filter -- null-year tracks are intentionally included.
# ---------------------------------------------------------------------------
_BEST_YEAR_EXPR = """CASE
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
END"""

# Used for 90's at Night (single show, play-level rows)
_SHOW_SQL = f"""
SELECT
    p.play_ts,
    p.station_show,
    ct.canonical_id,
    ct.display_artist,
    ct.display_title,
    ct.spotify_album_type,
    {_BEST_YEAR_EXPR} AS best_year
FROM plays p
JOIN plays_to_canonical ptc ON p.id = ptc.play_id
JOIN canonical_tracks   ct  ON ptc.canonical_id = ct.canonical_id
WHERE p.station_show = ?
"""

# Used for SEGMENT_SHOWS: includes play_date and play_hour for the segmentation groupby
_SEG_SQL = f"""
SELECT
    p.id                       AS play_id,
    p.play_ts,
    p.station_show,
    DATE(p.play_ts)            AS play_date,
    STRFTIME('%H', p.play_ts)  AS play_hour,
    ct.canonical_id,
    ct.display_artist,
    ct.display_title,
    ct.spotify_album_type,
    {_BEST_YEAR_EXPR} AS best_year
FROM plays p
JOIN plays_to_canonical ptc ON p.id = ptc.play_id
JOIN canonical_tracks   ct  ON ptc.canonical_id = ct.canonical_id
WHERE p.station_show IN ({{placeholders}})
ORDER BY p.play_ts
"""


def _get_oob_indices(tracks_df):
    """
    Return index labels of mid-segment OOB tracks from valid segments.

    "Mid-segment" means the track falls chronologically between the first and
    last in-band track in its block -- i.e. it is surrounded by segment tracks
    on both sides.  Tracks that appear before the segment starts (pre-bleed)
    or after the last in-band track (tail / intentional throwbacks) are
    excluded.  Blocks that fail to reach min_inband are skipped entirely.

    tracks_df must have columns: play_ts, station_show, play_date, play_hour,
    best_year. Additional columns are preserved via index labels.
    """
    oob_indices = []

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

        if len(in_band_indices) < min_inband:
            continue  # block did not form a valid segment -- skip

        in_band_set = set(in_band_indices)
        grp_idx_list = list(grp_sorted.index)
        pos_map = {idx: pos for pos, idx in enumerate(grp_idx_list)}
        first_ib_pos = pos_map[in_band_indices[0]]
        last_ib_pos  = pos_map[in_band_indices[-1]]

        for idx in grp_idx_list:
            if idx not in in_band_set and first_ib_pos < pos_map[idx] < last_ib_pos:
                oob_indices.append(idx)

    return oob_indices


def run_segment_breakers():
    print("=== Segment Breakers Report ===")
    conn = sqlite3.connect(DB_PATH)

    # ------------------------------------------------------------------
    # 90's at Night -- year-range check
    # ------------------------------------------------------------------
    df_90s = pd.read_sql_query(_SHOW_SQL, conn, params=[NINETIES_SHOW])
    df_90s["play_ts"] = pd.to_datetime(df_90s["play_ts"], errors="coerce")

    # One row per canonical: keep most recent play as representative
    df_90s_dedup = (
        df_90s
        .sort_values("play_ts", ascending=False)
        .drop_duplicates(subset=["canonical_id"], keep="first")
        .reset_index(drop=True)
    )

    is_null = df_90s_dedup["best_year"].isna()
    is_oob  = (~is_null) & (
        (df_90s_dedup["best_year"] < YEAR_LOW) | (df_90s_dedup["best_year"] > YEAR_HIGH)
    )
    df_90s_breakers = df_90s_dedup[is_null | is_oob].copy()
    df_90s_breakers["breach_reason"] = df_90s_breakers["best_year"].apply(
        lambda y: "year_null" if pd.isna(y) else "year_oob"
    )

    print(f"  90's at Night: {len(df_90s_breakers)} canonical breakers")
    print(f"    year_null : {is_null[is_null | is_oob].sum()}")
    print(f"    year_oob  : {is_oob[is_null | is_oob].sum()}")

    # ------------------------------------------------------------------
    # Segmented shows -- density OOB check
    # ------------------------------------------------------------------
    placeholders = ",".join("?" * len(SEGMENT_SHOWS))
    seg_sql = _SEG_SQL.format(placeholders=placeholders)
    df_seg = pd.read_sql_query(seg_sql, conn, params=list(SEGMENT_SHOWS))
    conn.close()
    df_seg["play_ts"] = pd.to_datetime(df_seg["play_ts"], errors="coerce")

    oob_idx = _get_oob_indices(df_seg)
    df_oob = df_seg.loc[oob_idx].copy() if oob_idx else df_seg.iloc[0:0].copy()
    df_oob["breach_reason"] = "oob_segment_track"

    print(f"  SEGMENT_SHOWS OOB plays: {len(df_oob)}")
    for show in SEGMENT_SHOWS:
        n = (df_oob["station_show"] == show).sum()
        print(f"    {show}: {n}")

    # Deduplicate OOB to one row per (canonical, show): keep most recent play
    if not df_oob.empty:
        df_oob_dedup = (
            df_oob
            .sort_values("play_ts", ascending=False)
            .drop_duplicates(subset=["canonical_id", "station_show"], keep="first")
            .reset_index(drop=True)
        )
    else:
        df_oob_dedup = df_oob.copy()

    print(f"  Unique OOB canonicals across SEGMENT_SHOWS: {df_oob_dedup['canonical_id'].nunique()}")

    # ------------------------------------------------------------------
    # Combine -- one row per canonical_id
    # If a canonical is a breaker in multiple shows, station_show and
    # breach_reason are aggregated as sorted unique values.
    # ------------------------------------------------------------------
    shared_cols = ["canonical_id", "display_artist", "display_title",
                   "spotify_album_type", "play_ts", "best_year",
                   "station_show", "breach_reason"]

    combined = pd.concat(
        [df_90s_breakers[shared_cols], df_oob_dedup[shared_cols]],
        ignore_index=True,
    )
    combined["play_ts"] = pd.to_datetime(combined["play_ts"], errors="coerce")
    combined = combined.sort_values("play_ts", ascending=False)

    final = (
        combined
        .groupby("canonical_id", sort=False)
        .agg(
            display_artist=("display_artist",    "first"),
            display_title=("display_title",      "first"),
            best_year=("best_year",              "first"),
            spotify_album_type=("spotify_album_type", "first"),
            most_recent_play_ts=("play_ts",      "first"),
            station_show=("station_show",
                          lambda x: "; ".join(sorted(x.unique()))),
            breach_reason=("breach_reason",
                           lambda x: "; ".join(sorted(x.unique()))),
        )
        .reset_index()
        .sort_values(
            ["station_show", "best_year", "most_recent_play_ts"],
            ascending=[True, True, False],
            na_position="first",
        )
        .reset_index(drop=True)
    )

    out_cols = ["canonical_id", "display_artist", "display_title", "best_year",
                "station_show", "most_recent_play_ts", "spotify_album_type", "breach_reason"]
    final = final[out_cols]

    out_path = OUTPUT_DIR / "segment_breakers.csv"
    final.to_csv(out_path, index=False, encoding="utf-8")

    print(f"\n  Total unique canonical breakers: {len(final)}")
    print(f"  Saved: {out_path}")
    print()
    return final


if __name__ == "__main__":
    run_segment_breakers()
