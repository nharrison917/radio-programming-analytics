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

## Show clustering: current scalar feature set (as of v1.7.3)

Six features, each covering a distinct dimension:

| Feature | Dimension |
|---|---|
| `avg_best_year` | Era center -- when the music is from |
| `exclusive_artist_pct` | Show identity -- how much of the artist roster appears nowhere else |
| `era_continuity_mean_gap` | Era mixing -- avg year gap between consecutive plays within a day |
| `era_spread` | Era breadth -- std dev of best_year; how wide the era window is |
| `rotation_depth` | Repeat cycle -- avg plays per unique canonical track |
| `band_age_score` | Career maturity -- composite: z-scored median + IQR of band_age, averaged |

**Removed and why:**
- `unique_artists_per_hour` -- airtime-contaminated (species-area problem; small shows score
  artificially high just from having fewer total hours)
- `artist_entropy` -- flat for 9/11 shows; the two outliers (90s at Night, This Just In)
  were already caught by era_spread and other features
- `freshness_pct` -- redundant with avg_best_year once era_spread is in the model

**Key clustering result:** k=3 cut gap improved from 1.002 (original) to 2.798.
Sunday Mornings Over Easy correctly re-classified to the weekday core (was held in the
specialty cluster by UAPH contamination). Cluster assignments: Cluster 1 = 10@10 shows +
90s at Night; Cluster 2 = weekday rotation + Sunday Mornings; Cluster 3 = This Just In.

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

### Show clustering scalar feature overhaul (v1.7.3)
- Diagnosed UAPH as airtime-contaminated (species-area problem) and artist_entropy as
  near-flat for 9/11 shows; both dropped.
- Diagnosed freshness_pct as redundant with avg_best_year + era_spread; dropped.
- Added era_spread (std dev of best_year): separates 90s at Night (3.7), This Just In
  (8.4), 10@10 (16.7), main rotation (19-21) without entropy.
- Added rotation_depth (plays per unique canonical track): captures repeat-cycle tightness.
  Peak Music (4.84) and This Just In (3.42) highest for different reasons.
- Replaced median_band_age with band_age_score composite (z-score median + IQR averaged).
- segment_breakers.csv: The La's "There She Goes" correctly dropped after last session's
  override resolved the wrong-version match.

### Repertoire metric: replaced binary top-N with TF-IDF (v1.7.4)
- Investigated per-show artist/track play counts to find where the "tail" begins for
  each show; determined widening TOP_N would add noise for shallow-rotation shows.
- Identified the conceptual flaw in binary top-N: ubiquitous rotation artists (REM,
  Oasis, RHCP, Beck, Black Crowes -- in all 11 shows) had equal weight to
  show-exclusive artists, inflating cross-cluster similarity.
- Replaced with TF-IDF cosine similarity on full vocabulary. IDF zeroes out the shared
  rotation backbone; TF weights by relative play frequency within a show.
- Fixed a latent bug in the process: previous `compute_repertoire_similarity()` re-queried
  the DB with raw plays, so SEGMENT_SHOWS were not using in-band tracks. Now accepts the
  already-segmented `df` from the caller.
- Key result: 10@10 pair 0.700 -> 0.928; 10@10 vs main rotation 0.40-0.47 -> 0.07-0.10.
  Cluster structure unchanged.
