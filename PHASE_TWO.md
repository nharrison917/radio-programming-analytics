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
  mb_first_release_year -> spotify_album_release_year

Tracks that remain unresolved after MB lookup are flagged as "year uncertain" in output.

---

## Stage 1 -- Schema migration

**File:** `scraper/db.py`

Add columns to `canonical_tracks` via `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`:

| Column | Type | Purpose |
|---|---|---|
| `spotify_album_type` | TEXT | "album", "single", or "compilation" |
| `spotify_isrc` | TEXT | ISRC from Spotify external_ids |
| `mb_first_release_year` | INTEGER | Earliest release year from MusicBrainz |
| `mb_lookup_status` | TEXT | PENDING / SUCCESS / FAILED / NO_ISRC / SKIPPED |
| `mb_looked_up_at` | TEXT | Timestamp of last MB lookup attempt |

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
4. If valid: store in `mb_first_release_year`, set `mb_lookup_status = "SUCCESS"`
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
    WHEN ct.mb_first_release_year IS NOT NULL
     AND ct.mb_first_release_year < ct.spotify_album_release_year
    THEN ct.mb_first_release_year
    ELSE ct.spotify_album_release_year
END AS best_year
```

All year-dependent queries switch from `spotify_album_release_year` to `best_year`.

### Uncertainty flag

Tracks where year is still uncertain after MB lookup:
  `spotify_album_type = "compilation" AND (mb_lookup_status != "SUCCESS"
    OR mb_first_release_year >= spotify_album_release_year)`
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
- No analytics crash when mb_first_release_year is NULL (CASE handles it)
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

3. 10@10 post-segment tracks: show-boundary problem is separate from year accuracy.
   Even with correct years, tracks after the 10-song segment are attributed to
   the wrong show. Documented as a known limitation; not addressed in Phase Two.
    - Possibly to be addressed in a future phase.

4. MusicBrainz community data: MB `first-release-date` is user-contributed and
   occasionally wrong or missing for obscure releases. Treated as best available,
   not authoritative.

---

## Future direction -- canonical_artists year accuracy (Phase Three candidate)

`canonical_artists.earliest_release_year` is currently populated by Spotify's artist
enrichment, which has the same remaster/compilation contamination problem as track
years. Spotify paginates through an artist's discography and finds the minimum album
year, but a remaster appearing as an "early album" can corrupt this.

**MusicBrainz provides a cleaner path:**

1. Map `spotify_artist_id` -> MB Artist MBID via the MB URL relationship endpoint:
   `GET /ws/2/url?resource=https://open.spotify.com/artist/{id}&inc=artist-rels`

2. Browse release-groups for that MBID, which carry original `first-release-date`:
   `GET /ws/2/release-group?artist={mbid}&limit=100`

3. Take the minimum valid `first-release-date` year across all release groups.

This is 2-3 calls per artist vs. many pages of Spotify pagination, and the dates are
original releases rather than remaster dates.

**Before implementing:** sample a set of already-enriched canonical_artists and compare
Spotify's `earliest_release_year` against the MB release-group result to quantify the
error rate. If the difference is small, the work may not be justified. If Spotify is
consistently returning remaster years as "earliest", it is.

**Schema additions needed:**
- `canonical_artists.mb_artist_id TEXT`
- `canonical_artists.mb_earliest_release_year INTEGER`
- `canonical_artists.mb_lookup_status TEXT`

**best_year rule applies here too:** only accept MB year if it is earlier than Spotify's.

---

## Phase Two implementation order

1. Stage 1 (schema) -- COMPLETE
2. Stage 2 (Spotify backfill + enrichment.py update) -- IN PROGRESS
   Backfill is rate-limited to ~600 records/day. Run daily until complete:
     python rs_main.py enrich-meta
     python rs_main.py mb-enrich
   ~4 more daily runs needed as of 2026-03-29 (600/2559 done).
3. Stage 3 (MusicBrainz lookup) -- COMPLETE for backfilled records; runs after each
   daily enrich-meta batch as above.
4. Stage 4 (analytics) -- COMPLETE. best_year CASE expression wired into all
   year-dependent queries; results improve incrementally as backfill completes.

Stages 1-2 are low risk. Stage 3 introduces a new external API dependency.
Stage 4 changes analytics outputs -- re-run all visuals after Stage 3 data is populated.

## Verification checklist

Run after Stage 4 implementation and again after the full backfill completes:

- [ ] `python rs_main.py mb-enrich` reports 0 eligible records (backfill done)
- [ ] David Bowie - Fame `best_year` = 1975 (MB correction correctly rejected)
- [ ] The Clash - I Fought The Law `best_year` = 1979 (MB correction correctly applied)
- [ ] The Allman Brothers Band - Jessica `best_year` = 1973 (corrected from 2013)
- [ ] **10 @ 10 era continuity % rises materially** from current 59.6% -- this is the
      primary end-to-end validation that year corrections are flowing through to analytics.
      Expect a significant increase once compilation/remaster years are corrected for
      the 1960s-era tracks that show dominates. If the number does not shift, investigate
      whether best_year is being applied in the era_continuity.py pairs query.
- [ ] Box plot release year distribution shifts earlier for older shows (Coach, Peak Music)
- [ ] Era continuity re-run prints pair count alongside continuity % so any reduction
      from uncertain-year exclusions is visible
