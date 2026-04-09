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

---

## MusicBrainz Title/Artist Search as a Year Correction Fallback

**Context:**
Phase Two's ISRC-based MB lookup corrects release years for tracks matched by Spotify
to compilations or remaster editions. It works by resolving the ISRC to a MB recording
and taking the earliest `first-release-date` across all linked recordings. However,
ISRC is version-specific: a remaster gets its own ISRC, distinct from the original.
When Spotify returns the remaster's ISRC, MB correctly reports the remaster year --
which is no improvement. The "only accept MB if earlier" rule prevents regression but
leaves those tracks uncorrected.

**The gap this addresses:**
Tracks where:
- `spotify_album_type = "compilation"` AND `mb_isrc_year >= spotify_album_release_year`
  (ISRC lookup returned a later or equal year -- no help)
- OR `mb_lookup_status = "FAILED"` or `"NO_ISRC"` (no ISRC available at all)

Examples from the dataset: Beatles "I Feel Fine" (Spotify=2000, MB ISRC returned 2000
because the ISRC points to the 2000 remaster recording -- original is 1964).

**Approach explored (2026-04-09):**
MB has a recording search endpoint:
```
GET /ws/2/recording?query=artist:"{artist}" AND recording:"{title}"&fmt=json&limit=25
```
Returns recordings by text search, each with a `first-release-date`. Filter results
to artist name fuzzy-match >= 88 (RapidFuzz token_set_ratio) and title fuzzy-match >= 88,
then take the minimum year across passing results. Accept only if strictly earlier than
current `best_year`.

**Test population:**
Ran on all SUCCESS canonical tracks for 7 artists from the 10@10 show, chosen for
catalog depth and era (pre-2000 tracks most affected by compilation matching):

| Artist | Tracks | Compilation tracks | Notes |
|---|---|---|---|
| The Beatles | 43 | 8 | Primary test case |
| The Rolling Stones | 32 | 6 | MB ISRC already corrected most; few gaps |
| Led Zeppelin | 32 | 0 | All matched to remaster albums; ISRC confirmed correct years |
| Tom Petty & The Heartbreakers | 26 | 8 | Several tracks with no MB data at all |
| The Who | 23 | 15 | Two tracks stuck at 2023 box-set year |
| R.E.M. | 34 | 4 | Mostly clean; compilations are live recordings |
| U2 | 41 | 13 | Heavy remaster use but ISRC confirmed correct years |

**Results on Beatles (43 tracks):**

| Title | cur_best | Search result | Assessment |
|---|---|---|---|
| Paperback Writer | 2000 | 1966 | Correct -- clean win |
| Let It Be | 1970 | 1969 | Plausible (single vs. album release date) |
| Two Of Us | 1970 | 1969 | Same as above |
| Day Tripper | 2000 | 1974 | Wrong -- should be 1965; landed on a reissue |
| Hey Jude | 2006 | 1978 | Wrong -- should be 1968; landed on a reissue |
| Hello Goodbye | 2000 | 1980 | Wrong -- should be 1967 |
| Lady Madonna | 2000 | 1989 | Wrong -- should be 1968 |
| I Feel Fine | 2000 | 1987 | Wrong -- should be 1964 |

8 of 43 tracks flagged as improvements; only 1-3 are genuinely correct.

**Root cause of failures:**
MB text search returns all recordings matching the name -- original pressings, reissues,
compilations, live versions -- and their release dates span decades. The naive minimum
year is the earliest *any* version appeared in MB, which is often a reissue year rather
than the original studio release. The ISRC approach is more precise because it pins to
a specific recording version; the text search approach casts too wide a net.

**Refinement path -- release-group type filtering:**
MB recording objects, fetched with `inc=releases+release-groups`, include every release
the recording appears on. Each release has a `release-group` with:
- `primary-type`: "Album", "Single", "EP", "Other", "Broadcast"
- `secondary-types`: list -- ["Compilation"], ["Live"], ["Remix"], etc. Empty = plain studio release.

Filter to `(primary-type = "Album" OR primary-type = "Single") AND secondary-types = []`.
This covers both original studio albums and original single releases, while excluding
remix singles, promo singles, live releases, compilations, and other non-original forms.
Taking the minimum date across only those releases should substantially improve precision.

The tradeoff: each recording search call becomes heavier (more data returned per result),
and the correct recording must appear in the 25-result limit. The goal is a genuine fix,
not just an incremental improvement -- accepting both album and single types maximizes
the chance of finding the true first release date.

**Implementation complete (2026-04-09):**

Both ISRC lookup and title/artist search are now implemented as two independent passes
in `scraper/mb_enrichment.py`. Results are stored separately:
- `mb_isrc_year` (renamed from `mb_first_release_year`) -- ISRC lookup result
- `mb_title_artist_year` -- title/artist search result
- `manual_year_override` -- human-verified override, highest priority

Eligibility broadened from compilations/remasters-only to all `spotify_status = 'SUCCESS'`
tracks. `best_year` CASE expression updated in all analytics files to use both sources.

**Bugs fixed during implementation:**
- Title/artist URL query now wraps field values in Lucene quotes (fixes 400 errors on
  artist names containing `-`, `&`, `(`, `)` and other Lucene special characters)
- ISRC normalized to uppercase before MB API call (fixes 400 errors from lowercase ISRCs
  stored by Spotify, e.g. `ushm90643902`)

**Remaining known gaps (require manual_year_override):**

| Artist | Title | Current | Should be | Why automated fails |
|---|---|---|---|---|
| Solomon Burke | Got To Get You Off My Mind | 2019 | 1965 | 1965 single in MB attributed to Eric Donaldson; artist fuzzy fails |
| The Who | I'm Free (Movie Version) | 1969 | 1975 | Spotify matched to original Tommy (1969), not the movie recording |
| The Beach Boys | Do You Wanna Dance? | 2012 | 1965 | ISRC points to 2012 reissue; MB confirms 2012 |
| Sugarloaf | Don't Call Us, We'll Call You | 2019 | 1975 | ISRC lookup FAILED in MB |

**Next steps:**
1. Run `python rs_main.py mb-enrich` to backfill both passes (~75 min for ~2,700 tracks)
2. After backfill: run validation query (see 10@10 section below) to identify remaining outliers
3. Apply `manual_year_override` for the tracks listed above and any new outliers found

---

## 10 @ 10 Segment Boundary Detection

**Context:**
Stage 5 of PHASE_TWO.md calls for filtering bleed tracks from 10@10 era continuity
computation. The station attributes plays by clock hour, but the 10@10 segment
(a themed 10-song block from a specific year) does not fill the full hour. Pre-show
tracks from the prior program and post-segment regular-rotation tracks contaminate
the continuity pairs, artificially depressing the measured continuity %.

**Analysis conducted 2026-04-09 -- full dataset, 122 blocks:**

Track count distribution (tracks attributed to the 10@10 clock hour):

| Count | Blocks | % |
|---|---|---|
| 9-10 | 14 | 11.5% -- clean, no bleed |
| 11-12 | 76 | 62.3% -- typical: 1-2 bleed tracks |
| 13-15 | 32 | 26.2% -- heavy bleed |

Leading (pre-segment) bleed: 82% of blocks have none; 14% have 1; rare cases have 2-3.
Trailing (post-segment) bleed: 17% have none; most have 1-4; the trailing end is
the dominant contamination problem.

**Algorithm design -- density-based era detection:**

The naive approach (median of best_year +/- 3 years) fails. Two Feb 11 blocks
demonstrated the failure mode: the segment is clearly 1979-1980, but outlier tracks
(wrong-year canonicals within the segment + post-bleed at 2025) pull the median to
1987, which falls between the two clusters. The +-3 band then excludes every era track
and the algorithm marks the entire block as out-of-band.

The correct approach: find the year Y in the actual track data that maximizes the
count of tracks with best_year in [Y-3, Y+3]. This is the densest cluster and is
robust to a small number of outliers as long as the era tracks outnumber them.

End-of-segment detection: after accumulating N in-band tracks, two consecutive
out-of-band tracks signal the end. "Two consecutive" is the right threshold for
distinguishing isolated within-segment anomalies (single wrong-year tracks) from
genuine bleed runs. However -- see the blocking issue below.

**Blocking issue: recurring wrong-year canonical tracks**

The "two consecutive OOB" rule is fragile against specific canonical tracks that
appear in-segment but have wrong best_year values due to unresolved compilation/
remaster matching. These tracks recur identically across every instance of the
same themed block (the station reuses the same playlist), so they produce false
segment breaks in every affected block. 14 of 122 blocks would be falsely cut.

Tracks identified as the primary offenders (canonical_id lookup needed -- match
by display_artist + display_title):

| Artist | Title | Shows as | Should be | Appears in |
|---|---|---|---|---|
| The Beach Boys | Do You Wanna Dance? | 2012 | 1965 | 1965-66 blocks |
| The Moody Blues | Go Now | 2014 | 1964 | 1965-66 blocks |
| Solomon Burke | Got To Get You Off My Mind | 2019 | 1965 | 1965-66 blocks |
| Eric Burdon & The Animals | Inside - Looking Out | 1983 | 1966 | 1965-66 blocks |
| The Spencer Davis Group | Keep On Running (Radio Session, 1966) | 2004 | 1966 | 1965-66 blocks |
| The Bobby Fuller Four | I Fought The Law | 1998 | 1966 | 1965-66 blocks |
| Sugarloaf | Don't Call Us, We'll Call You | 2019 | 1975 | 1974-75 blocks |
| The Who | I'm Free (Movie Version) | 1969 | 1974 | 1974-75 blocks |
| M | Pop Muzik (Nik Launay '79 12") | 2022 | 1979 | 1979-80 blocks |
| The Inmates | Dirty Water | 1994 | ~1979 | 1979-80 blocks |
| Nick Lowe | Stick It Where The Sun Don't Shine | 2017 | 1982 | 1981-82 blocks |

These tracks all fall into the category described in the MB Title/Artist Search
section above: compilation or session-matched, ISRC lookup returned a remaster/
reissue year, and no correction applied. The release-group filter approach in
that section is the right fix for most of them.

**Implementation order:**

1. Fix year data for the tracks listed above (via MB title/artist search with
   release-group filtering, or manual overrides for the small count that are
   clearly wrong). This is prerequisite -- segmentation written against bad year
   data will silently produce wrong results on every repeat of those themed blocks.
2. Implement density-based era detection in analytics/era_continuity.py.
3. Apply start/end boundary detection to filter bleed plays from continuity pairs
   at query time. Do not modify the plays table.
4. Validate: re-run era continuity for 10@10 and confirm the % rises materially
   from the current 59.6% baseline (the primary end-to-end check from PHASE_TWO.md).

**Status: Design complete. Partially unblocked (2026-04-09).**

Year data infrastructure is now in place (dual MB passes + manual_year_override).
Next step before implementing density-based detection: run the MB backfill, then
build and run a validation query to identify which of the listed offenders are
corrected automatically vs. still need manual overrides. Only after manual overrides
are applied for the recurring offenders should the segmentation algorithm be implemented
-- otherwise it will silently produce wrong results on every repeat of those themed blocks.

**Validation query (run after mb-enrich backfill):**
```sql
-- Tracks appearing in 10@10 blocks with best_year far from the block's era
-- (requires era detection to be implemented first to know the target year per block)
-- Interim: spot-check by looking at tracks where best_year is > 5yr from
-- the show's modal era, grouped by station_show
SELECT display_artist, display_title,
       spotify_album_release_year,
       mb_isrc_year, mb_title_artist_year, manual_year_override,
       CASE ... END AS best_year  -- full best_year expression
FROM canonical_tracks ct
JOIN plays_to_canonical ptc ON ct.canonical_id = ptc.canonical_id
JOIN plays p ON ptc.play_id = p.id
WHERE p.station_show IN ('10 @ 10', '10 @ 10 Weekend Replay', '90''s at Night')
ORDER BY best_year DESC;
```
