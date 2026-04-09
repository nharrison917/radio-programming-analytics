# Phase Two: Release Year Accuracy via ISRC + MusicBrainz

## Problem statement

`canonical_tracks.spotify_album_release_year` reflects the release year of whichever
album Spotify matched the track to -- not the original recording year. When Spotify
matches to a compilation or remastered edition, the year is wrong by decades.

Example: "I Fought The Law" (Bobby Fuller Four, 1966) matched to a 1998 compilation
-> stored as 1998. The error is systematic for older catalog and is directly
contaminating era-continuity and release year analytics.

## Solution approach

Two new data fields from sources we already have access to:

- `spotify_album_type` (album / single / compilation) -- flags low-confidence year records
- `spotify_isrc` -- the International Standard Recording Code; stable identifier
  we can cross-reference to MusicBrainz

MusicBrainz `first-release-date` is the earliest known release date for a recording
across all its appearances. It is the correct field for "when was this song originally
released." Coverage is strongest for pre-2000 catalog -- exactly the population most
affected by the compilation problem.

Analytics layer resolves year via a priority chain:
  manual_year_override -> min(mb_isrc_year, mb_title_artist_year) -> spotify_album_release_year

Tracks that remain unresolved after MB lookup are flagged as "year uncertain" in output.

---

## Stage 1 -- Schema migration

**File:** `scraper/db.py`

Add columns to `canonical_tracks` via `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`:

| Column | Type | Purpose |
|---|---|---|
| `spotify_album_type` | TEXT | "album", "single", or "compilation" |
| `spotify_isrc` | TEXT | ISRC from Spotify external_ids |
| `mb_isrc_year` | INTEGER | Earliest year from MusicBrainz ISRC lookup |
| `mb_lookup_status` | TEXT | SUCCESS / FAILED / NO_ISRC (ISRC pass) |
| `mb_looked_up_at` | TEXT | Timestamp of last MB ISRC lookup attempt |
| `mb_title_artist_year` | INTEGER | Earliest year from MB title/artist search |
| `mb_ta_status` | TEXT | SUCCESS / FAILED (title/artist pass) |
| `manual_year_override` | INTEGER | Human-verified correct year; overrides all automated sources |

Migration must be safe to run on an existing DB (idempotent). SQLite `ADD COLUMN`
is non-destructive. Existing SUCCESS records get `mb_lookup_status = NULL` until
Stage 3 runs.

New enrichment runs (Stage 2 forward) populate `spotify_album_type` and `spotify_isrc`
at Spotify enrichment time, so only a one-time backfill is needed for existing records.

**Acceptance criteria:**
- `init_db()` creates columns on a fresh DB
- Migration script runs cleanly on the live DB without touching existing data
- Columns visible in PRAGMA table_info output

---

## Stage 2 -- Spotify backfill (album_type + ISRC)

**New file:** `scraper/spotify_backfill.py`
**Entry point:** `python rs_main.py enrich-meta` (or run standalone)

Uses the Spotify single track endpoint (`GET /tracks/{id}`) per record.
The batch endpoint (`GET /tracks?ids=`) was deprecated by Spotify and returns 403
with client credentials; single-track calls are the only supported path.

Each track object returns:
- `external_ids.isrc`
- `album.album_type`

Scope: all `canonical_tracks` where `spotify_status = "SUCCESS"` and
`spotify_isrc IS NULL` (i.e., not yet backfilled).

Expected volume: ~2,500 tracks at 0.3s/call + 10s cooldown per 50 tracks (~16 min).
One-time operation; future enrichments populate these fields at enrich time.

**Spotify daily quota:** observed limit of ~600 calls/day on client credentials.
The backfill is idempotent -- re-running picks up where it left off. Full backfill
of 2,559 tracks requires ~5 daily runs. Run: `python rs_main.py enrich-meta`.

Write `spotify_isrc` and `spotify_album_type` back to `canonical_tracks`.

**Also update `enrichment.py`:** for all new enrichments going forward, fetch
and store `isrc` and `album_type` at enrichment time so no future backfill is needed.

**Remaster heuristic:** even `album_type = "album"` records can have unreliable years
if the album name contains any of these signals (case-insensitive substring match):
  `remaster`, `deluxe`, `anniversary`, `expanded`, `edition`

Flag these at query time from `spotify_album_name` -- do not store a derived flag in
the DB. Signals chosen from evidence against the actual catalog (see measured scope
table below). `live`, `greatest hits`, `collection` were evaluated and excluded:
live album years are usually correct; the others had 0-1 matches in this dataset.

**Measured scope (from 600-track backfill sample, 2026-03-29):**

| album_type  | Count | Remaster | Deluxe | Anniversary | Any flag | Flag % |
|---|---|---|---|---|---|---|
| album       |   509 |       68 |     30 |           4 |       96 |  18.9% |
| compilation |    53 |       10 |     10 |           5 |       13 |  24.5% |
| single      |    38 |        0 |      0 |           0 |        0 |   0.0% |

Key findings:
- `single` is clean -- zero remaster/deluxe flags. Most reliable year source.
- `compilation` is the primary problem but also carries remaster signals (24.5%).
- `album` at 19% flagged is the larger surprise: 96 tracks with album_type="album"
  still have unreliable years due to remaster/deluxe editions. These are missed if
  filtering on album_type alone. The remaster heuristic is necessary, not optional.

**Acceptance criteria:**
- All SUCCESS canonicals have `spotify_isrc` populated (or NULL where Spotify has none)
- All SUCCESS canonicals have `spotify_album_type` populated
- `enrichment.py` populates both fields on new enrichments
- Backfill is idempotent (re-running skips already-populated records)

---

## Stage 3 -- MusicBrainz lookup

**New file:** `scraper/mb_enrichment.py`
**Entry point:** `python rs_main.py mb-enrich` (or wired into weekly run)

### Scope

Tracks eligible for MB lookup:
  `spotify_album_type = "compilation"`
  OR `spotify_album_name` matches remaster heuristic (see Stage 2)

Tracks excluded:
  `spotify_isrc IS NULL` -> set `mb_lookup_status = "NO_ISRC"`, skip
  `mb_lookup_status IN ("SUCCESS", "SKIPPED")` -> do not retry
  `mb_lookup_status = "FAILED"` -> retry after N days (same pattern as Spotify retry)

### MusicBrainz API

Endpoint: `https://musicbrainz.org/ws/2/isrc/{isrc}?fmt=json`

Returns recordings linked to that ISRC, each with `releases` containing
`first-release-date`. Take the earliest valid year across all recordings.

Rate limit: 1 request/second. Add `User-Agent` header identifying the project
(MB requirement -- requests without User-Agent are rejected).
  User-Agent: radio-scraper/1.0 (contact from .env)

### Year resolution logic

1. Parse `first-release-date` from MB response (may be YYYY, YYYY-MM, or YYYY-MM-DD)
2. Extract year component
3. Validate against plausibility bounds (1920 to current_year + 1) -- same rule as Spotify
4. If valid: store in `mb_isrc_year`, set `mb_lookup_status = "SUCCESS"`
5. If MB returns no usable date: `mb_lookup_status = "FAILED"`
6. If ISRC not found in MB: `mb_lookup_status = "FAILED"`

### Output

After each run, print a summary:
  - Tracks looked up
  - SUCCESS / FAILED / NO_ISRC counts
  - Sample of corrected years (before -> after, largest corrections first)

**Acceptance criteria:**
- Bobby Fuller Four "I Fought The Law" corrects from 1998 -> 1966 (or similar)
- Beatles "I Feel Fine" corrects from 2000 -> 1964/65
- Rate limiting respected (no 429 responses)
- Lookup is idempotent
- Tracks with no ISRC are correctly marked NO_ISRC, not FAILED

---

## Stage 4 -- Analytics layer

**Files affected:**
- `analytics/era_continuity.py`
- `analytics/analysis.py`
- `analytics/visuals.py`
- `analytics/boxplot_release_year.py`
- `analytics/heatmap_avg_release_year.py`

### best_year resolution

**Corrected logic (updated 2026-03-29 after Stage 3 validation run):**

Simple COALESCE is wrong. ISRCs are version-specific -- a remaster gets its own ISRC.
When Spotify correctly matches a track to the original album (e.g. album_year=1975),
it can still return the ISRC of the remaster version. MB then correctly reports the
remaster year for that ISRC, making things worse. Observed examples from first MB run:
  - David Bowie - Fame: Spotify=1975 (correct), MB=2016 (remaster ISRC)
  - Fleetwood Mac - Silver Springs: Spotify=1977 (correct), MB=2004

**Rule: only accept MB when it pushes the year earlier than Spotify.**
If MB returns a year >= spotify_album_release_year, Spotify was already right (or
MB is reporting a remaster year for a different ISRC version). Discard it.

```sql
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
```

All year-dependent queries switch from `spotify_album_release_year` to `best_year`.

### Uncertainty flag

Tracks where year is still uncertain after MB lookup:
  `spotify_album_type = "compilation" AND (mb_lookup_status != "SUCCESS"
    OR mb_isrc_year >= spotify_album_release_year)`
  OR remaster heuristic match AND same condition

These can be:
- Excluded from era continuity pairs (with a note on reduced pair count)
- Or included with a visual flag in hover text ("year uncertain")

Decision deferred to implementation -- both approaches are valid. Recommend
excluding from pair computation for accuracy, surfacing the exclusion count.

### CLAUDE.md update

Update the schema table entry for `canonical_tracks` to include the five new columns.
Update "Data integrity rules" section with the best_year resolution logic.

**Acceptance criteria:**
- Era continuity numbers change materially for 10@10 (expected: continuity % rises)
- Box plot release year distribution shifts earlier for older shows
- No analytics crash when mb_isrc_year or mb_title_artist_year is NULL (CASE handles it)
- David Bowie - Fame remains at 1975 (MB correction rejected, not applied)

---

## Expected coverage

MB lookup will help most where the problem is worst:
- Pre-1980 catalog matched to compilations: high MB coverage, large year corrections
- 1980-2000 catalog: good MB coverage
- Post-2000: lower MB coverage, but compilation matching is less common here anyway

Based on the 600-track backfill sample (2026-03-29):
- compilation: 53 tracks (8.8%)
- album with remaster/deluxe flag: 96 tracks (16.0%)
- Total year-suspect: 149 of 600 (24.8%)

Extrapolated to the full 2,559 SUCCESS catalog: ~635 tracks with suspect years.
This is higher than the original 20-30% compilation-only estimate because the
remaster/deluxe "album" type population was not anticipated at planning time.

Not all will have ISRCs; not all ISRCs will resolve in MB.
Realistic expectation: year corrections on 15-20% of all enriched tracks.

---

## Known limitations after Phase Two

1. **ISRC version-specificity**: ISRCs are assigned per recording version, not per
   song. A remaster has a different ISRC from the original. Spotify may return the
   remaster ISRC even when correctly displaying the original album year. MB then
   correctly reports the remaster year for that ISRC. The "only accept MB if earlier"
   rule handles this, but means those tracks get no MB correction -- they rely on
   Spotify's album year being accurate, which it usually is in these cases.

2. `album_type = "album"` remaster editions not caught by ISRC alone -- the remaster
   heuristic (album name string match) is approximate. A 2009 Rubber Soul remaster
   returns `album_type = "album"` and the heuristic catches it via "Remastered" in
   the name, but bespoke anniversary titles may slip through.

3. 10@10 show-boundary and alias problem: two related issues contaminate era continuity
   for this show. See Stage 5 below.

4. MusicBrainz community data: MB `first-release-date` is user-contributed and
   occasionally wrong or missing for obscure releases. Treated as best available,
   not authoritative.

---

## Stage 5 -- 10 @ 10 show boundary and alias fix

**Files affected:**
- `analytics/era_continuity.py`
- Possibly `analytics/analysis.py` and `analytics/visuals.py` (if show filtering is shared)

### Two distinct problems

**Problem A: Show alias -- "10 @ 10 Weekend Replay" treated as a separate show**

The weekend replay of 10@10 is logged under a different `station_show` value than
the weekday version. Era continuity groups by `station_show`, so the two are computed
separately. The weekend replay has fewer play pairs, producing a noisier and
misleadingly distinct continuity reading. They represent the same programming format
and should be combined.

Fix: normalize "10 @ 10 Weekend Replay" -> "10 @ 10" at analytics query time using
a CASE expression or a `show_aliases` mapping in config. Do not alter the `plays`
table -- preserve the original `station_show` value for observability. Apply the
alias at the analytics layer only.

**Problem B: Post-segment track bleed**

The 10@10 segment is a 10-song block embedded in a larger broadcast hour. Tracks
played before or after the segment within the same hour may be captured under the
same `station_show = "10 @ 10"` label depending on how the playlist page structures
the hour. These non-segment tracks contaminate the era continuity pairs:
- A post-segment track from regular rotation creates a pair with the last 10@10 song
- That pair spans two programming contexts and almost always breaks era continuity
- This depresses the measured continuity % for 10@10 artificially

Fix options (pick one after inspecting the data):
- **Option A:** Filter to the first 10 play pairs per hour for 10@10 (assumes the
  segment always runs first in the hour -- verify against the data)
- **Option B:** Inspect `play_ts` offsets within the hour; the 10@10 segment likely
  occupies a predictable time window (e.g. first 30 minutes)
- **Option C:** Accept bleed as unfixable without segment markers in the source data;
  document it and exclude 10@10 from continuity comparisons in the final output

**Before implementing:** run a query to count plays per hour per `station_show = "10 @ 10"`
and inspect whether extra tracks beyond 10 are consistently present and where they
fall in the timestamp sequence.

### Acceptance criteria

- "10 @ 10 Weekend Replay" plays are included in "10 @ 10" era continuity computation
- 10@10 era continuity % changes materially after the segment boundary fix (direction
  depends on whether bleed was helping or hurting -- verify)
- Original `station_show` value is preserved in the `plays` table unchanged
- Show alias mapping is defined in config, not hardcoded in the analytics script

---

## Phase Two implementation order

1. Stage 1 (schema) -- COMPLETE
2. Stage 2 (Spotify backfill + enrichment.py update) -- COMPLETE
   All 2,726 SUCCESS tracks have spotify_isrc and spotify_album_type populated (2026-04-09).
   enrich-meta and mb-enrich remain part of the weekly cadence for new tracks going forward.
3. Stage 3 (MusicBrainz lookup) -- COMPLETE. 743 SUCCESS, 43 FAILED, 0 eligible remaining.
   mb-enrich runs weekly to process newly enriched tracks (small batch ongoing).
4. Stage 4 (analytics) -- COMPLETE. best_year CASE expression wired into all
   year-dependent queries.
5. Stage 5 (10@10 show boundary + alias) -- PENDING

Stages 1-2 are low risk. Stage 3 introduces a new external API dependency.
Stage 4 changes analytics outputs -- re-run all visuals after Stage 3 data is populated.

## Verification checklist

Run after Stage 4 implementation and again after the full backfill completes:

- [x] `python rs_main.py mb-enrich` reports 0 eligible records (backfill done -- 2026-04-09)
- [x] David Bowie - Fame `best_year` = 1975 (MB correction correctly rejected -- mb=2016, spotify=1975, rule keeps 1975)
- [x] The Clash - I Fought The Law `best_year` = 1979 (MB correction correctly applied -- mb=1979, spotify=2013)
- [x] The Allman Brothers Band - Jessica `best_year` = 1973 (corrected from 2013 -- mb=1973)
- [ ] **10 @ 10 era continuity % rises materially** from current 59.6% -- this is the
      primary end-to-end validation that year corrections are flowing through to analytics.
      Expect a significant increase once compilation/remaster years are corrected for
      the 1960s-era tracks that show dominates. If the number does not shift, investigate
      whether best_year is being applied in the era_continuity.py pairs query.
- [ ] Box plot release year distribution shifts earlier for older shows (Coach, Peak Music)
- [ ] Era continuity re-run prints pair count alongside continuity % so any reduction
      from uncertain-year exclusions is visible
- [ ] "10 @ 10 Weekend Replay" plays are included in "10 @ 10" era continuity output
- [ ] 10@10 era continuity % changes after segment boundary investigation -- direction
      and magnitude documented
