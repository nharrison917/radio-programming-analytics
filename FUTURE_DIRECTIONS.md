# TBD -- Open Questions and Deferred Decisions

## Show Attribution Fidelity vs. Programmatic Intent

**Context:**
The scraper attributes plays to shows by hour, matching the station website's structure.
Several shows have a structural overflow problem where the scraped hour-boundary does not
align cleanly with the actual show boundary.

**Known cases:**

- **"This Just In with Meg White"** -- A new-music show (21:00 hour) that consistently
  ends its last ~10 minutes (:50-:59) with 1-2 older throwback tracks. This appears to be
  intentional programming, not a data error. The show is correctly attributed; the host
  just plays classics before handoff.

- **"90's at Night"** -- Largely clean. A handful of non-90s plays appear at the very
  start of the 20:00 hour, likely bleed from whatever aired before. Most apparent
  anomalies are actually 90s tracks on remasters/compilations where Spotify returns the
  reissue year (MB enrichment resolves most of these).

- **"10 @ 10" and "10 @ 10 Weekend Replay"** -- Addressed via density-based segmentation
  (see `analytics/era_continuity.py`). The segmentation detects and excludes bleed tracks
  at query time for continuity analysis. The underlying `plays` data is unchanged.

**The open question:**
The website only identifies shows by hour. When a show's actual content does not fill
or fit neatly into that hour (overflow, early end, late start, throwback segment), the
scraped data reflects *what the website reported*, not necessarily the station's
programmatic intent.

Do we correct the data to recover "true intent" -- e.g., reclassify :50-:59 tracks
from "This Just In" as belonging to the next show -- or do we treat the data as an
honest record of what the website said at scrape time?

**Status: Undecided.** Both positions are defensible:
- "As recorded" is reproducible and makes no assumptions about intent.
- "True intent" is more analytically meaningful for per-show content analysis.

Any correction strategy would need to be documented clearly and applied consistently,
since it changes the meaning of `station_show` in the dataset.

---

## Era Sensitivity Analysis for "90's at Night" and "This Just In with Meg White"

**"This Just In with Meg White" -- COMPLETE (2026-04-10)**

Added to `SEGMENT_SHOWS` in v1.2.0. The default segmentation parameters (BAND=3,
MIN_INBAND=8, CONSEC_OOB=2) work without modification: modal era lands at ~2025,
the throwback tail (1-2 tracks at :50-:59, pre-2020) is cleanly excluded in all
43/43 observed blocks. Segmentation rationale: the throwback tail is intentional
programming by Meg White, but for show-identity analytics (era position, freshness)
it is noise -- the same consistency argument that applies to 10@10.

Results: `avg_best_year` 2023.2 -> 2025.8, `freshness_pct` 0.901 -> 1.000,
`era_continuity_mean_gap` 3.40 -> 0.34.

---

**"90's at Night" -- examined 2026-04-10, decided NOT to segment**

Exploratory analysis of 193 enriched plays across 8 airing dates (16 hour-blocks)
showed the show is already clean:

- 96.4% of plays fall within 1988-2005 (the expected 90s range)
- Only 7/193 tracks are OOB; they appear at scattered positions (4, 5, 8, 11) --
  not front-loaded at the start of the 20:00 hour as originally hypothesised
- The OOB tracks are post-2005 modern tracks, not a systematic bleed pattern

Segmentation would produce nearly identical metrics and adds complexity without
analytical value. **Not added to `SEGMENT_SHOWS`.**

If the dataset grows substantially and a pattern emerges, the approach would require
a wider band (~7yr to cover the full decade) and possibly a fixed center (1995) rather
than density-inferred modal era. See structural notes below.

---

**Integration pattern for future segmented shows (established 2026-04-10, updated 2026-04-10):**

All architectural prerequisites from v1.1.0 are now resolved. Adding a new show requires:

1. **Parameter config** -- Add a show-specific entry to `SEGMENT_PARAMS` in
   `era_continuity.py` if the show needs different band/min/consec values. Falls back
   to "default" automatically for shows not listed.

2. **SEGMENT_SHOWS** -- Add the show name to the `SEGMENT_SHOWS` tuple. This cascades
   automatically through `load_segmented_tracks()` (dynamic SQL), `display_df` asterisk
   labeling in `run_era_continuity`, `_display_label` in `show_clustering`, and the
   `era_continuity_mean_gap` override loop.

3. **Verify block validity** -- Run the per-block trace to confirm the expected fraction
   of blocks produce valid segments before committing.

**"90's at Night" -- structural notes if revisited:**
`_modal_era` infers era from density. For a fixed-era format show with a full-decade
range, a fixed center (1995, band ~7) would be more principled than density inference.
The consecutive-OOB termination rule still applies. A simple temporal filter (exclude
first N minutes of the 20:00 hour) may also be worth testing given the hypothesis that
any bleed is front-loaded.

**Status: 90's at Night deferred; This Just In complete.**
