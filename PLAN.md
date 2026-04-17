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

## Pending review: primary_artist_mismatch.csv output

First run produced 90 mismatches at threshold=75. The report is working correctly --
real wrong-version catches sort to the top, predictable false-positive clusters sit
below them. A human review pass should:

1. **Actionable at the top (score < 30):**
   - canonical_id=2279: Roger Daltrey / Behind Blue Eyes -> primary=The Chieftains (score 8)
     -- Chieftains covered it; wrong version stored. Needs override.
   - canonical_id=2087: Lo Fidelity Allstars / Battle Flag -> primary=Pigeonhed (score 20)
     -- Pigeonhed was the collab vocalist; Spotify credited them first. Needs override.
   - canonical_id=2674: O.A.R. & Robert Randolph... / Fool In The Rain -> primary=O.A.R. (score 13)
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

## What was done this session

### Documentation and presentation overhaul (v1.7.6)
- ANALYSIS.md: rewrote clustering section with current feature set (era_spread,
  rotation_depth, band_age_score replacing UAPH/entropy/freshness_pct), TF-IDF
  repertoire method with quantified improvement, updated cluster assignments
  (Sunday Mornings in weekday core), feature values table from CSV, two supporting images.
- README.md: refreshed dataset stats to 2026-04-17 (67 days, 19,174 plays, 98.4%,
  512 MB corrections, 923 artists). Updated clustering description to TF-IDF and named
  features. Replaced broken density_vs_freshness.png with repertoire heatmap. Fixed
  misleading "interactive output files" reference. Added cluster and mb-artist-enrich
  entry points. Added scikit-learn and scipy to tech stack.
- show_clustering.py: _shorten_label() helper abbreviates long show names across all
  dendrograms and heatmaps. Annotation moved above chart (y=1.08). automargin=True +
  r=80 fixes right-side label cutoff.
- docs/images/: clustering_scalar_dendrogram.png and clustering_repertoire_heatmap.png
  added as tracked static assets for documentation.
