# PLAN.md - Next Session Pickup

## Primary planned work: Primary Artist Mismatch Report

### Problem
Our Spotify enrichment scores artist matches using:
```python
a_score = max(similarity(norm_artist, a) for a in spotify_artist_norms)
```
This takes the max similarity across ALL credited artists on a track, not just the primary.
This is correct for matching (finds "feat. Aerosmith" tracks when searching for Aerosmith),
but it means wrong-version matches (covers, collabs) score 100/100 on attempt 1 and are
invisible to the existing `enrichment_attempt_3_4.csv` report.

The primary artist stored in `canonical_tracks.spotify_primary_artist_name` comes from
`artists[0]` (Spotify's primary), which may not be the artist that scored the 100.

Confirmed cases from this session where this caused problems:
- "Aerosmith" scraped, Run-D.M.C. stored as primary (Walk This Way collab version)
- "The La's" scraped, CYRIL stored as primary (CYRIL cover of There She Goes)
- "Band Of Gypsys" scraped, "The Return Of The Band Of Gypsys" stored (different act)

These all showed as band_age negatives in the new quality reports, which is how we found
them. The mismatch report would catch them proactively.

### Proposed report: `quality_checks/primary_artist_mismatch.csv`

**Approach:**
- Query all `spotify_status = 'SUCCESS'` canonical tracks
- Normalize both `display_artist` and `spotify_primary_artist_name` using the existing
  `normalize_artist()` from `scraper/normalization_logic.py`
- Compare them using RapidFuzz `token_sort_ratio()` or `ratio()` -- NOT `token_set_ratio`
  (token_set_ratio allows subset matching and would score "Band Of Gypsys" vs
  "The Return Of The Band Of Gypsys" as 100, which defeats the purpose)
- Flag rows where the direct score < threshold (suggest starting at 75, tune from there)
- Include `play_count` per canonical_id (join via plays_to_canonical)
- Sort by score ascending (worst mismatches first), then play_count descending

**Columns to include:**
- `canonical_id`, `display_artist`, `display_title`
- `spotify_primary_artist_name`
- `primary_artist_score` (the direct ratio between display_artist and spotify_primary_artist_name)
- `spotify_artist_score` (the existing stored score -- the max-over-all-artists one)
- `play_count`
- `spotify_match_attempt`
- `spotify_album_release_year`

**Known noise to expect (false positives):**
- Punctuation/formatting: "Run-D.M.C." vs "Run DMC", "AC/DC" variants
- "The ..." prefix differences
- Legitimate solo credit shifts (artist credited differently on radio vs Spotify)

These cluster obviously on first review. Threshold and any exclusion list can be tuned
after seeing the first real output.

**Where to generate it:**
Generate in `scraper/weekly.py` at the end of the weekly run, alongside the existing
`enrichment_attempt_3_4.csv` report. Both live in `analytics/outputs/quality_checks/`
which is git-tracked.

---

## Pending pipeline runs (must happen before next analysis)

Three tracks have overrides applied but need pipeline runs to fully resolve:

### 1. Aerosmith - Walk This Way (canonical_id=1818)
- Override applied: Spotify ID `4JfuiOWlWCkjP6OKurHjSn` (1975 Toys in the Attic version)
- `mb_isrc_year` and `mb_lookup_status` already cleared (ready for mb-enrich)
- **Required:** `python rs_main.py weekly` then `python rs_main.py mb-enrich`
- Expected result: primary artist → Aerosmith, best_year → 1975, band_age → ~2yr

### 2. The La's - There She Goes (canonical_id=803)
- Override applied: Spotify ID `0D3Th2YU14U737O0nSHXT8` (1990 original)
- All MB fields cleared (mb_isrc_year, mb_lookup_status, mb_title_artist_year, mb_ta_status)
- **Required (in order):**
  1. `python rs_main.py weekly` -- updates primary artist to The La's
  2. `python rs_main.py set-artist-meta --artist-name "The La's" --mb-id "ff3e88b3-7354-4f30-967c-1a61ebc8c642"`
  3. `python rs_main.py mb-enrich` -- picks up 1990 ISRC and year
- Expected result: primary artist → The La's, best_year → 1990, band_age → ~7yr (career start 1983)

---

## What was done this session (for context)

### band_age quality reports added (analytics/band_age.py)
Two new CSVs written at end of every `python rs_main.py analyze` run:
- `quality_checks/band_age_negative.csv` -- tracks with band_age < -2 (threshold: BAND_AGE_NEG_THRESHOLD)
- `quality_checks/band_age_extreme.csv` -- tracks with band_age > 50 (threshold: BAND_AGE_POS_THRESHOLD)
One row per canonical_id, sorted worst-first. Columns include best_year, career_start_year,
career_start_source, play_count, mb_isrc_year, mb_title_artist_year.

### Data corrections made this session
- **Spoon - Wild (canonical_id=1367):** Cleared false `mb_title_artist_year=1990`
  (MB title/artist search matched a different artist's "Wild"). mb_ta_status kept SUCCESS
  to prevent retry. best_year now 2021 from mb_isrc_year. No pipeline run needed.
- **Rob Thomas - 3 Am (canonical_id=1773):** Cleared false `mb_isrc_year=1994`
  (MB associated a 2005 ISRC with the original Matchbox Twenty demo). mb_lookup_status
  kept SUCCESS to prevent retry. best_year now 2005 from Spotify. No pipeline run needed.
- **Band Of Gypsys - Them Changes (canonical_id=71):** Set spotify_status=NO_MATCH,
  manual_year_override=1970. Closed "The Return Of The Band Of Gypsys" in canonical_artists
  with mb_artist_status=NO_MATCH. No correct Spotify track exists; excluded from band_age.
- **CYRIL** in canonical_artists: still has its own row from the wrong La's match --
  may appear in enrichment reports. Not explicitly closed but the track override will
  detach it once weekly runs.

### Root cause analysis (three distinct failure modes)
1. **Spotify match lands on wrong version** (collab/cover): artist scoring uses max over
   all credited artists, so the wrong version scores perfectly. Primary artist stored is
   Spotify's `artists[0]`, not the matching artist. Invisible to attempt_3_4 report.
2. **MB ISRC returns wrong year**: MB associates an ISRC with a different recording than
   intended (Rob Thomas case). Affects best_year only, not artist attribution.
3. **MB title/artist false match**: Common song titles match to different artists' recordings
   in MB text search (Spoon "Wild" case). Affects best_year only.

---

## Known bug: add-override silently ignored for SUCCESS tracks

`enrichment.py` only processes tracks where `spotify_status IN ('PENDING', 'FAILED')`
and `spotify_album_id IS NULL`. Tracks with a wrong SUCCESS match have a valid
`spotify_album_id` and SUCCESS status, so the override in `manual_spotify_overrides`
is never applied.

Workaround (documented in MANUAL_OVERRIDE.md): set the track to FAILED and clear
`spotify_album_id` and `spotify_last_attempted_at` before running weekly.

Proper fix: expand the enrichment WHERE clause to also select tracks that have a row
in `manual_spotify_overrides`, regardless of current status:
```sql
WHERE (
    spotify_album_id IS NULL
    AND spotify_status IN ('PENDING', 'FAILED')
    AND (spotify_last_attempted_at IS NULL OR spotify_last_attempted_at < DATETIME('now', '-2 days'))
)
OR canonical_id IN (SELECT canonical_id FROM manual_spotify_overrides)
```
Note: after applying an override, the row stays in `manual_spotify_overrides` (no applied_at
column), so the track would be re-fetched on every weekly run. Either delete the row after
applying or add an `applied_at` column to prevent redundant re-fetches.

## Files changed this session (uncommitted)
- `analytics/band_age.py` -- quality reports added, diagnostic columns added to _load_data
- `analytics/outputs/quality_checks/band_age_negative.csv` -- new, git-tracked
- `analytics/outputs/quality_checks/band_age_extreme.csv` -- new, git-tracked
- Various DB corrections (not committable)
- `scraper/config.py` -- has unstaged changes from before this session (unknown what changed)

Suggested commit grouping:
- Commit 1: `analytics/band_age.py` + the two new quality CSVs
- Commit 2: `scraper/config.py` separately (check what changed first)
