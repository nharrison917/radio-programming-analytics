# PLAN.md -- 10@10 Segmentation: Propagation to Charts and Clustering

## Status: COMPLETE (2026-04-10, extended 2026-04-10)

---

## Problem

`era_continuity.py` already computes density-segmented metrics for `10 @ 10` and
`10 @ 10 Weekend Replay`, but those results only appear in the terminal printout and a
CSV. The three HTML charts still use unfiltered pairs, showing ~8.7 yr / ~9.1 yr mean
gaps instead of the correct ~0.6 yr.

`show_clustering.py` computes all scalar features (avg_best_year, freshness_pct,
artist_entropy, era_continuity_mean_gap, etc.) from ALL plays -- including bleed tracks
that air before and after the themed 10@10 segment. Those bleed tracks are station
filler (not selected by the same DJ) and distort every scalar feature for these shows.

## Goal

1. All era_continuity charts display segmented values for SEGMENT_SHOWS.
2. All show_clustering scalar features use only in-band tracks for SEGMENT_SHOWS.
3. Segmented shows are labeled `<name> *` in all charts, with chart annotation
   `"* = density-segmented pairs"`.
4. No database data is deleted; segmentation is an analytical filter only.

## Affected files

- `analytics/era_continuity.py`
- `analytics/show_clustering.py`

## Background / key parameters (already in era_continuity.py)

```python
SEGMENT_BAND = 3              # +/- yr window for in/out-of-band classification
SEGMENT_MIN_INBAND = 8        # min in-band tracks to declare a valid segment
SEGMENT_CONSECUTIVE_OOB = 2   # consecutive OOB tracks that signal segment end
SEGMENT_SHOWS = ("10 @ 10", "10 @ 10 Weekend Replay")
```

The `_modal_era(years)` and `_segment_block(years)` functions already work correctly.
The existing `compute_segmented_metrics` function already produces correct pair-level
metrics. What's missing is: (a) row-level track filtering, (b) merging segmented values
back into the display dataframe, (c) asterisk labeling, (d) chart annotations.

---

## Step 1 -- Extend `load_10at10_tracks` SQL (era_continuity.py)

Add `p.id AS play_id`, `ct.canonical_id`, `ct.norm_artist` to the SELECT clause.

These columns are needed by `show_clustering.py` when it replaces its plays dataframe
rows with the filtered in-band tracks.

Current columns returned: `play_ts`, `station_show`, `play_date`, `play_hour`, `best_year`
New columns to add: `play_id`, `canonical_id`, `norm_artist`

---

## Step 2 -- Add `get_inband_tracks(tracks_df)` to era_continuity.py

New public function. Place it after `_segment_block` and before `load_10at10_tracks`.

```python
def get_inband_tracks(tracks_df):
    """
    Apply density-based segmentation per (station_show, play_date, play_hour) block.
    Returns a DataFrame containing only in-band tracks from valid segments.

    tracks_df must have columns: play_ts, station_show, play_date, play_hour, best_year
    Any additional columns (play_id, canonical_id, norm_artist) are preserved.
    """
    keep_indices = []

    for (show, date, hour), grp in tracks_df.groupby(
        ["station_show", "play_date", "play_hour"]
    ):
        grp_sorted = grp.sort_values("play_ts")
        years = grp_sorted["best_year"].tolist()
        modal = _modal_era(years)
        if modal is None:
            continue

        in_band_indices = []
        consecutive_oob = 0
        for idx, y in zip(grp_sorted.index, years):
            if y is not None and abs(y - modal) <= SEGMENT_BAND:
                in_band_indices.append(idx)
                consecutive_oob = 0
            else:
                consecutive_oob += 1
                if consecutive_oob >= SEGMENT_CONSECUTIVE_OOB:
                    break

        if len(in_band_indices) >= SEGMENT_MIN_INBAND:
            keep_indices.extend(in_band_indices)

    return tracks_df.loc[keep_indices].copy()
```

This mirrors the logic in `_segment_block` but tracks original dataframe row indices
instead of collecting year values, so the full row (with all columns) is preserved.

---

## Step 3 -- Update `compute_segmented_metrics` to add `avg_era` and `mid_pct`

`chart_fingerprint` needs `avg_era`; `chart_buckets` needs `mid_pct`. Currently missing.

In the `for y1, y2 in zip(in_band, in_band[1:])` loop, also collect mid_era:

```python
for y1, y2 in zip(in_band, in_band[1:]):
    all_pairs.append({
        "station_show": show,
        "gap": abs(y2 - y1),
        "mid_era": (y1 + y2) / 2.0,   # ADD THIS
    })
```

In the `metrics_rows.append(...)` block, add:

```python
era_cont_pct  = round(100.0 * (gaps <= CONTINUITY_THRESHOLD).sum() / len(gaps), 1)
era_break_pct = round(100.0 * (gaps > BREAK_THRESHOLD).sum() / len(gaps), 1)
metrics_rows.append({
    "station_show":        show,
    "total_pairs":         len(gaps),
    "mean_abs_gap":        round(gaps.mean(), 2),
    "era_continuity_pct":  era_cont_pct,
    "era_break_pct":       era_break_pct,
    "mid_pct":             round(100.0 - era_cont_pct - era_break_pct, 1),   # ADD
    "avg_era":             round(grp["mid_era"].mean(), 0),                  # ADD
})
```

Note: the existing code builds `era_continuity_pct` and `era_break_pct` inline in the
append dict. Refactor to named variables first so `mid_pct` can reference them.

---

## Step 4 -- Build display df in `run_era_continuity` and pass to charts

After `seg_metrics, block_stats = compute_segmented_metrics(tracks_10at10)` and before
`print_segmented_comparison(...)`, insert:

```python
# Build display df: replace SEGMENT_SHOWS rows with segmented values + asterisk labels
display_df = df.copy()
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
```

Then replace the three chart calls (currently using `df`) with `display_df`:

```python
chart_mean_gap(display_df)       # was chart_mean_gap(df)
chart_fingerprint(display_df)    # was chart_fingerprint(df)
chart_buckets(display_df)        # was chart_buckets(df)
```

Keep `df` (unmodified) for the CSV export and for `print_segmented_comparison`.
The CSV should not have asterisks in the show names.

---

## Step 5 -- Add annotation to each chart function

In `chart_mean_gap`, `chart_fingerprint`, and `chart_buckets`, add to `fig.update_layout`:

```python
annotations=[dict(
    text="* = density-segmented pairs",
    xref="paper", yref="paper",
    x=0.0, y=-0.06,
    showarrow=False,
    font=dict(size=11, color="#666666"),
    xanchor="left",
)],
```

Adjust `y` if the annotation overlaps chart content (more negative = lower).

---

## Step 6 -- Update `show_clustering.py`

### 6a: Add import at the top of the file (after the existing sys.path.insert line)

```python
from analytics.era_continuity import (
    load_10at10_tracks,
    get_inband_tracks,
    SEGMENT_SHOWS,
)
```

The project root is already on sys.path via the existing `sys.path.insert` call, so
`analytics.era_continuity` resolves correctly.

### 6b: Filter plays in `run_show_clustering` immediately after `_load_plays()`

```python
df = _load_plays()

# Replace SEGMENT_SHOWS rows with in-band filtered rows
tracks_10 = load_10at10_tracks()
inband = get_inband_tracks(tracks_10)
df = df[~df["station_show"].isin(SEGMENT_SHOWS)].copy()
inband_sub = inband[
    ["play_id", "play_ts", "station_show", "canonical_id", "norm_artist", "best_year"]
].copy()
df = pd.concat([df, inband_sub], ignore_index=True)
df["play_ts"] = pd.to_datetime(df["play_ts"], errors="coerce")
```

### 6c: Asterisk display labels

Add a module-level helper after the import block:

```python
def _display_label(show):
    return f"{show} *" if show in SEGMENT_SHOWS else show
```

At each point where show name lists are passed to chart functions, apply the helper:

- Before calling `_dendrogram(dist_cond_scalar, shows_scalar, ...)`:
  `display_scalar = [_display_label(s) for s in shows_scalar]`
  Pass `display_scalar` instead of `shows_scalar` as the `labels` argument.

- Before `_scalar_heatmap(scalar_scaled, scalar_df, shows_scalar, ...)`:
  Pass `display_scalar` instead of `shows_scalar`.

- Before `_dendrogram(dist_cond_rep, shows_rep, ...)`:
  `display_rep = [_display_label(s) for s in shows_rep]`
  Pass `display_rep`.

- Before the combined and equal-weight dendrograms that use `common_shows`:
  `display_common = [_display_label(s) for s in common_shows]`
  Pass `display_common`.

- In `_similarity_heatmap`: the `shows` list comes from `sim_df.index`. Apply the same
  mapping before passing to the Heatmap x/y and text fields. Easiest: rename the
  sim_df index/columns before calling:
  ```python
  sim_labeled = sim_df.rename(
      index=_display_label, columns=_display_label
  )
  _similarity_heatmap(sim_labeled, ...)
  ```
  Then inside `_similarity_heatmap`, `shows = list(sim_df.index)` will already have
  the asterisks.

**Critical:** do NOT rename show names in the dataframes used for distance matrix
computation or scalar feature indexing. Only rename in the final `labels` / axis lists
passed to chart functions. The `common_shows` alignment logic (`set(shows_scalar) &
set(shows_rep)`) uses original names and must not see asterisks.

### 6d: Add annotation to clustering chart functions

Add the same annotation dict to `_dendrogram`, `_scalar_heatmap`, and
`_similarity_heatmap` layouts:

```python
annotations=[dict(
    text="* = density-segmented pairs",
    xref="paper", yref="paper",
    x=0.0, y=-0.08,
    showarrow=False,
    font=dict(size=11, color="#666666"),
    xanchor="left",
)],
```

---

## Extension: "This Just In with Meg White" (2026-04-10)

Added in v1.2.0 after exploratory analysis confirmed the show has a clean, predictable
throwback tail (1-2 tracks at :50-:59, pre-2020) that the existing segmentation logic
handles correctly with unchanged parameters (BAND=3, MIN_INBAND=8, CONSEC_OOB=2).

Changes made:
- `SEGMENT_SHOWS` extended to include "This Just In with Meg White".
- Global constants (`SEGMENT_BAND`, etc.) replaced with `SEGMENT_PARAMS` dict keyed
  by show name; `_show_params(show)` helper for lookup with "default" fallback.
- `TRACKS_SQL_10AT10` renamed to `_TRACKS_SQL_TEMPLATE` (dynamic `{placeholders}`).
- `load_10at10_tracks()` renamed to `load_segmented_tracks()` (parameterized SQL,
  derives show list from `SEGMENT_SHOWS` at call time).
- `_segment_block` and `get_inband_tracks` updated to accept/look up explicit params.
- `era_continuity_10at10_segmented.csv` renamed to `era_continuity_segmented.csv`.

Results for "This Just In with Meg White" (43/43 blocks valid):
  avg_best_year: 2023.2 -> 2025.8 (inband tracks only)
  freshness_pct: 0.901 -> 1.000
  era_continuity_mean_gap: 3.40 -> 0.34

"90's at Night" was assessed and explicitly NOT added: data is 96.4% in-era
(7/193 OOB tracks), bleed is scattered (not front-loaded), no meaningful metric
distortion. See FUTURE_DIRECTIONS.md.

---

## Verification checklist

Run: `python rs_main.py analyze`

- [ ] `era_continuity_mean_gap.html`: "10 @ 10 *" bar shows ~0.6 yr, not ~8.7 yr
- [ ] `era_continuity_mean_gap.html`: "10 @ 10 Weekend Replay *" shows ~0.6 yr, not ~9.1 yr
- [ ] `era_continuity_fingerprint.html`: same shows in correct position, avg_era ~1978
- [ ] `era_continuity_buckets.html`: ~98.7% tight bucket for both shows
- [ ] All three era_continuity charts have "* = density-segmented pairs" annotation
- [ ] Terminal: `print_segmented_comparison` still prints correctly (uses raw `df`)
- [ ] `era_continuity.csv`: show names do NOT have asterisks
- [ ] Clustering charts: 10@10 shows labeled with `*` on all axes/labels
- [ ] Clustering charts: `era_continuity_mean_gap` feature reflects ~0.6 yr for 10@10
- [ ] `show_clustering_features.csv`: check that avg_best_year for 10@10 shows is now
     in the ~1975-1985 range (was inflated by bleed tracks from current rotation)
- [ ] `show_clustering_features.csv`: freshness_pct for 10@10 shows is now near 0%
     (correct -- these shows play old music)
- [ ] No Python errors or warnings during the run

---

## Notes

- `canonical_tracks` was created directly in SQLite; `norm_artist` is a real column
  (used successfully in `show_clustering._load_plays()`) even though it is not listed in
  the CLAUDE.md schema summary.
- The `compute_scalar_features` inline `era_sql` in `show_clustering.py` becomes
  redundant once the plays dataframe is pre-filtered. However, it still computes
  `era_continuity_mean_gap` for ALL shows (not just SEGMENT_SHOWS). Since the 10@10
  rows are now replaced in `df` before `compute_scalar_features` runs, the inline SQL
  result for 10@10 will still be the unfiltered value -- and then gets overwritten by
  `era_gap` from the SQL. Wait -- actually the inline SQL queries the database directly;
  it does not use the filtered `df`. So the `era_continuity_mean_gap` for 10@10 in
  show_clustering will STILL be the unfiltered value unless we explicitly override it.

  **Fix:** After `compute_scalar_features(df)` returns `scalar_df`, override the
  10@10 rows:
  ```python
  from analytics.era_continuity import (
      load_10at10_tracks, get_inband_tracks, compute_segmented_metrics, SEGMENT_SHOWS
  )
  # (compute_segmented_metrics also imported)
  tracks_10 = load_10at10_tracks()   # already called above; reuse if possible
  seg_metrics, _ = compute_segmented_metrics(tracks_10)  # or reuse from get_inband_tracks call
  for _, seg_row in seg_metrics.iterrows():
      show = seg_row["station_show"]
      if show in scalar_df.index:
          scalar_df.loc[show, "era_continuity_mean_gap"] = seg_row["mean_abs_gap"]
  ```

  To avoid calling `load_10at10_tracks()` twice, load it once and pass to both
  `get_inband_tracks` and `compute_segmented_metrics`.

- The `show_clustering_features.csv` export captures the final scalar values, so
  checking it is a good way to verify the era_continuity override landed correctly.
