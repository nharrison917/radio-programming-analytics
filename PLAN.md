# PLAN.md - Next Session Pickup

## Outstanding item: add-override silently ignored for SUCCESS tracks

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

---

## Future feature: band_age as a show clustering dimension

The band_age boxplot already shows meaningful per-show variation -- some shows
skew toward veteran artists at the peak of their career; others play recent material
from newly-established acts. This is a different signal from avg_best_year (which
tells you *when* the track was recorded) and could sharpen the clustering.

**What to add:** a `median_band_age` (or mean) scalar feature to `show_clustering.py`.
The per-show stats are already computed and saved in `band_age_summary.csv` -- the
clustering script could read that CSV and join on show name, rather than re-querying.

**Considerations before implementing:**
- Coverage varies by show. A show with only 40% MB-covered plays should probably not
  contribute its band_age feature, or should be down-weighted. The summary CSV
  includes `coverage_pct` and `mb_pct` -- set a minimum threshold (suggest 70% MB)
  before including the feature.
- Segmented shows (`SEGMENT_SHOWS`) appear as `<name> *` in the summary CSV. The
  clustering script uses plain show names internally. Either strip the asterisk suffix
  when joining, or align the label convention.
- Decide whether to use `median_band_age` (more robust to outliers) or `mean_band_age`.
  Given the spread in the boxplot, median is probably the right call.
- Normalise before including in the combined distance matrix (same z-score approach as
  the other scalar features).

---

## Pending review: primary_artist_mismatch.csv output

First run produced 90 mismatches at threshold=75. The report is working correctly --
real wrong-version catches sort to the top, predictable false-positive clusters sit
below them. A human review pass should:

1. **Actionable at the top (score < 30):**
   - canonical_id=2279: Roger Daltrey / Behind Blue Eyes → primary=The Chieftains (score 8)
     -- Chieftains covered it; wrong version stored. Needs override.
   - canonical_id=2087: Lo Fidelity Allstars / Battle Flag → primary=Pigeonhed (score 20)
     -- Pigeonhed was the collab vocalist; Spotify credited them first. Needs override.
   - canonical_id=2674: O.A.R. & Robert Randolph... / Fool In The Rain → primary=O.A.R. (score 13)
     -- Fool In The Rain is a Led Zeppelin song. Check whether this is an OAR cover or
        a false low score because display_artist includes extra band names.

2. **Known false-positive clusters (no action needed):**
   - `Matchbox 20` vs `Matchbox Twenty` (name variant, score 61) -- 4 rows
   - `CSNY` vs `Crosby, Stills, Nash & Young` (abbreviation, score 25) -- 1 row
   - `Paul McCartney & Wings` vs `Wings` or `Paul McCartney` (credit ambiguity) -- ~6 rows
   - `Neil Young & Crazy Horse` vs `Neil Young` (band-suffix, scores ~55) -- 6 rows
   - `Bob Seger & The Silver Bullet Band` vs `Bob Seger` (same pattern, score 40)
   - `Stevie Ray Vaughan & Double Trouble` vs `Stevie Ray Vaughan` (score 65) -- 9 rows
     Worth spot-checking: are these pointing to greatest-hits releases (solo credit)
     or the actual studio albums? If solo-credited GH compilations, those are wrong versions.

3. **Tune threshold if needed:** `PRIMARY_MISMATCH_THRESHOLD = 75` in
   `analytics/primary_artist_mismatch.py`. Current 90-row output may be slightly wide;
   raising to 80 would drop the SRV/Crazy Horse clusters if they're confirmed false positives.

---

## What was done in recent sessions

### Primary Artist Mismatch Report (this session)
- `analytics/primary_artist_mismatch.py` -- new module; generates
  `quality_checks/primary_artist_mismatch.csv` at end of every `analyze` run
- `rs_main.py` -- wired in `run_primary_artist_mismatch()` to the analyze block
- Approach: `normalize_artist()` both sides, score with `token_sort_ratio` (not
  `token_set_ratio`), flag below threshold=75. 90 mismatches on first run.

### band_age quality reports (previous session)
- `analytics/band_age.py` -- two quality CSVs added:
  - `quality_checks/band_age_negative.csv` (band_age < -2)
  - `quality_checks/band_age_extreme.csv` (band_age > 50)
- These reports surfaced the wrong-version catches that motivated the mismatch report.

### Data corrections (previous session)
- **Spoon - Wild (canonical_id=1367):** Cleared false `mb_title_artist_year=1990`.
- **Rob Thomas - 3 Am (canonical_id=1773):** Cleared false `mb_isrc_year=1994`.
- **Band Of Gypsys - Them Changes (canonical_id=71):** Set NO_MATCH + manual_year_override=1970.
- **Aerosmith - Walk This Way (canonical_id=1818):** Override applied + pipeline run completed.
- **The La's - There She Goes (canonical_id=803):** Override applied + pipeline run completed.
- **CYRIL** in canonical_artists: still has its own row from the wrong La's match --
  not explicitly closed; will detach from active tracks now that the La's override is resolved.

### Root cause taxonomy (three distinct failure modes)
1. **Wrong version** (collab/cover): artist scoring uses max over all credited artists,
   so the wrong version scores perfectly. Now caught by primary_artist_mismatch.csv.
2. **MB ISRC returns wrong year**: MB associates an ISRC with a different recording
   (Rob Thomas case). Affects best_year only.
3. **MB title/artist false match**: Common song titles match different artists in MB
   text search (Spoon "Wild" case). Affects best_year only.

---

## Files to commit (uncommitted as of this session)

- `analytics/primary_artist_mismatch.py` -- new
- `analytics/outputs/quality_checks/primary_artist_mismatch.csv` -- new, git-tracked
- `rs_main.py` -- wired in mismatch report
- `analytics/band_age.py` -- quality reports added (from previous session)
- `analytics/outputs/quality_checks/band_age_negative.csv` -- new (previous session)
- `analytics/outputs/quality_checks/band_age_extreme.csv` -- new (previous session)
- `scraper/config.py` -- has unstaged changes from before previous session (check diff first)

Suggested commit grouping:
- Commit 1: `analytics/band_age.py` + `band_age_negative.csv` + `band_age_extreme.csv`
- Commit 2: `analytics/primary_artist_mismatch.py` + `primary_artist_mismatch.csv` + `rs_main.py`
- Commit 3: `scraper/config.py` separately (verify diff first)
