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

**Context:**
The density-based era segmentation built for 10@10 (see `analytics/era_continuity.py`)
proved effective at isolating the themed segment from clock-hour bleed. The same approach
may be applicable to the other two format shows, with parameter adjustments to match each
show's structure.

**"90's at Night"**
The show has a defined era constraint (the 1990s, roughly 1988-2002 in practice). A
band-based filter -- flag plays where `best_year` falls outside a configurable window --
could quantify how much non-90s content appears and whether it is systematic (e.g.
consistently at the start of the 20:00 hour) or scattered. The segmentation logic would
be simpler than 10@10: the era is fixed by format, not inferred per block from density.

**"This Just In with Meg White"**
The show is a new-music show (21:00 hour), so the "era" is the present year +/- a short
window. The throwback tracks (:50-:59 slot) are a known intentional segment. Whether
to filter them depends on the analysis goal: for freshness metrics they are noise; for
understanding the show's full programming arc they are signal. The segmentation approach
could identify and separate the throwback tail from the main new-music block.

**Implementation note:**
The 10@10 segmentation lives in a dedicated section of `era_continuity.py`. The same
file is the natural home for these extensions. The core `_modal_era` / `_segment_block`
helpers are already factored for reuse.

**Architectural consideration (flagged 2026-04-10):**
The current segmentation parameters (`SEGMENT_BAND`, `SEGMENT_MIN_INBAND`,
`SEGMENT_CONSECUTIVE_OOB`) are global constants tuned for 10@10's structure: a tight
single-year cluster ~10 tracks deep. "90's at Night" and "This Just In" have different
structures -- a fixed decade window and a new-music + throwback-tail format respectively
-- and would likely need different band widths and minimum counts. Adding them to
`SEGMENT_SHOWS` as-is would apply the wrong parameters. Before expanding the show list,
the architecture should be extended to support a per-show parameter config (e.g., a dict
keyed by show name) rather than global constants. That is a small but deliberate
refactor, not just a one-line addition.

**Integration pattern (established 2026-04-10):**
The full end-to-end wiring is now in place for 10@10. Adding a new segmented show
requires the following touch points -- none are large, but all must be coordinated:

1. **Parameter config** -- `SEGMENT_BAND`, `SEGMENT_MIN_INBAND`, and
   `SEGMENT_CONSECUTIVE_OOB` are currently global constants tuned for 10@10. Before
   adding a second show, convert these to a per-show dict (keyed by show name) so each
   show can have its own band width and threshold. The dict lookup falls back to a
   global default for shows not explicitly configured.

2. **Data load** -- `load_10at10_tracks()` has a hardcoded `WHERE station_show IN
   ('10 @ 10', '10 @ 10 Weekend Replay')` clause. Either generalize it to
   `load_segmented_tracks(shows)` accepting a show list, or add a parallel load
   function for the new show. The SQL must also return `play_id`, `canonical_id`, and
   `norm_artist` (already present since the 2026-04-10 update) so `show_clustering`
   can splice the filtered rows back in.

3. **SEGMENT_SHOWS** -- Add the new show name to the `SEGMENT_SHOWS` tuple in
   `era_continuity.py`. This cascades automatically: `display_df` asterisk labeling
   in `run_era_continuity`, and `_display_label` in `show_clustering` both key off
   this tuple.

4. **show_clustering override** -- After `compute_scalar_features`, the inline SQL
   for `era_continuity_mean_gap` queries the raw DB and must be overridden with the
   segmented value. The override loop already handles all shows in `seg_metrics`, so
   a new show's segmented result is picked up automatically -- as long as the new
   show's tracks are passed to `compute_segmented_metrics`.

**"90's at Night" -- structural note:**
`_modal_era` infers the target era from density within each block. For a fixed-era
format show the target era is known in advance (roughly 1988-2002 in practice). Using
`_modal_era` is fine as a cross-check but the band would need to be wide enough to
cover the full decade, which is structurally different from 10@10's single-year cluster.
Consider a fixed-center alternative: skip `_modal_era` entirely and use a configurable
`center_year` + `band` pair, then apply the same consecutive-OOB termination rule.
The out-of-era plays at the start of the 20:00 hour are likely clock-hour bleed from
the preceding show -- a temporal filter (exclude the first N minutes of the hour) may
be simpler and more principled than a density filter for this case.

**"This Just In with Meg White" -- structural note:**
The throwback tail (:50-:59) is intentional programming, not bleed. Whether to segment
depends entirely on the analysis goal. For freshness metrics it is noise; for
understanding the full show arc it is signal. A density filter would work mechanically
(new-music block is a tight cluster around the current year; throwback tracks are
clear outliers) but the decision is whether the "segmented" version should be the
default or an alternate view. This is a framing question, not an implementation one.
Resolve the intent before writing code.

**Status: Deferred.** Validate that the 10@10 segmentation is stable across a larger
dataset before extending the pattern. Revisit once Phase Three enrichment is complete
and the dataset has grown further.
