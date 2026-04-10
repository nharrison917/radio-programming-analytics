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
file is the natural home for these extensions, with `SEGMENT_SHOWS` expanded or a
parallel per-show config structure introduced. The core `_modal_era` / `_segment_block`
helpers are already factored for reuse.

**Status: Deferred.** Validate that the 10@10 segmentation is stable across a larger
dataset before extending the pattern. Revisit once Phase Three enrichment is complete
and the dataset has grown further.
