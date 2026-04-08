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

- **"10 @ 10" and "10 @ 10 Weekend Replay"** -- The station plays ~10 songs from a
  specific era. Because the scraper assigns shows by hour, there is always some overflow:
  tracks from adjacent hours bleed in, and the set may also straddle the hour boundary,
  causing tracks from the 10 @ 10 block to be attributed to the preceding or following
  show.

**The open question:**
The website only identifies shows by hour. When a show's actual content does not fill
or fit neatly into that hour (overflow, early end, late start, throwback segment), the
scraped data reflects *what the website reported*, not necessarily the station's
programmatic intent.

Do we correct the data to recover "true intent" -- e.g., reclassify :50-:59 tracks
from "This Just In" as belonging to the next show, or strip overflow plays from "10 @ 10"
blocks -- or do we treat the data as an honest record of what the website said at
scrape time?

**Status: Undecided.** Not yet addressed. Both positions are defensible:
- "As recorded" is reproducible and makes no assumptions about intent.
- "True intent" is more analytically meaningful for per-show content analysis.

Any correction strategy would need to be documented clearly and applied consistently,
since it changes the meaning of `station_show` in the dataset.
