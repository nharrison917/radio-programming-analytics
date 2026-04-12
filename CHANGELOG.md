# Changelog

All notable changes to this project will be documented here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

Development assisted by Claude Code (Anthropic).

---

## [1.4.0] - 2026-04-11

Failure CSV automation and `set-meta` closure semantics.

### Added
- `scraper/weekly.py`: `analytics/outputs/spotify_failed.csv` is now regenerated
  automatically at the end of every Spotify enrichment run. Previously the file
  was a static snapshot; it will now always reflect the current FAILED set after
  each `python rs_main.py weekly`.
- `scraper/mb_enrichment.py`: `analytics/outputs/mb_failed.csv` is now regenerated
  automatically at all exit points of `run_mb_enrichment()` (normal completion and
  both aborted paths). Written via `_write_mb_failed_csv()`, which opens its own
  connection after the enrichment connection closes, so it works cleanly at every
  exit point.

### Changed
- `scraper/overrides.py`: `set-meta --year` on a `FAILED` track now also sets
  `spotify_status = 'NO_MATCH'`. Setting a year manually is an authoritative
  closure signal -- the track is confirmed not resolvable via Spotify -- so
  continuing to retry it is wasteful. The confirmation display shows the
  `FAILED -> NO_MATCH` transition explicitly. Tracks with statuses other than
  `FAILED` (SUCCESS, PENDING, NON_MUSIC, existing NO_MATCH) are unaffected.
- `scraper/mb_enrichment.py`: both Pass 1 (ISRC) and Pass 2 (title/artist)
  eligibility queries now exclude tracks where `manual_year_override IS NOT NULL`.
  Since `manual_year_override` takes unconditional priority in `best_year`,
  running MB lookups against a track that already has a hand-verified year is
  pure waste. The MB status columns (`mb_lookup_status`, `mb_ta_status`) remain
  accurate records of what MB actually returned and are not modified.
- `scraper/mb_enrichment.py`: `_write_mb_failed_csv()` query likewise excludes
  `manual_year_override IS NOT NULL` rows -- a track whose year is already
  resolved manually is not actionable regardless of its MB lookup status.

### Fixed
- DB: 7 existing `FAILED` canonicals that had `manual_year_override` set (from
  prior `set-meta` runs before the closure logic existed) were migrated to
  `NO_MATCH` in a one-time update, consistent with the new semantics.

---

## [1.3.0] - 2026-04-11

Manual override CLI and Phase Three schema additions.

### Added
- `scraper/overrides.py`: two commands for hand-correcting individual records without
  touching the enrichment pipeline:
  - `run_add_override(canonical_id, spotify_id)` -- inserts into `manual_spotify_overrides`;
    next `weekly` run enriches the track fully (year, duration, ISRC, artist ID). For tracks
    on Spotify where our search failed due to normalization errors or title mismatches.
  - `run_set_meta(canonical_id, year_raw, duration_raw)` -- writes `manual_year_override`,
    `manual_release_date`, and/or `manual_duration_ms` directly to `canonical_tracks`. For
    tracks not on Spotify. Accepts YYYY or YYYY-MM-DD for year; M:SS for duration.
    Both commands display current track state and require `y` confirmation before writing.
- `scraper/db.py`: Phase Three migration -- `manual_duration_ms INTEGER` and
  `manual_release_date TEXT` added to `canonical_tracks`. Idempotent; triggers automatically
  on first use of either override command via `migrate_db()`.
  - `manual_duration_ms`: separate from `spotify_duration_ms` so a future Spotify match
    cannot silently overwrite a hand-entered value.
  - `manual_release_date`: audit trail for full date precision (YYYY-MM-DD if known); does
    not affect `best_year` logic. Integer year extracted into existing `manual_year_override`.
- `rs_main.py`: `add-override` and `set-meta` modes; `--id`, `--spotify-id`, `--year`,
  `--duration` flags wired in.
- `PHASE_THREE.md`: Manual Override CLI section documenting both commands, schema additions,
  triage reference table (which command to use for which failure type).

### Context
Preceded by a full triage of the 43-record Spotify FAILED set and 89-record MB FAILED set.
Failure taxonomy established: station-branded sessions, covers, normalization failures,
version-suffix blocking MB search, recent releases not yet indexed, genuinely obscure tracks.
The CLI addresses the normalization and obscure-track categories. The version-suffix MB
search fix (Group B, ~26 records) is a separate upcoming code change in `mb_enrichment.py`.

---

## [1.2.3] - 2026-04-11

### Fixed
- `scraper/normalization_logic.py`: `normalize_for_key` now also converts U+0092
  (BREAK PERMITTED HERE) and U+0091 C1 control characters to straight apostrophes
  before removal. These are mis-decoded Windows-1252 smart quotes: the source website
  sent byte `0x92` (RIGHT SINGLE QUOTATION MARK in CP1252), which the scraper decoded
  as UTF-8 byte sequence `C2 92` (= U+0092). The 1.2.2 U+2019 fix did not catch this
  variant; one track ("Don't Dream It's Over" by The Head And The Heart) was affected.
- DB: re-normalized all 12 plays whose `norm_key_core` still held broken values from
  before the 1.2.2 fix. Root cause: `normalize_new_plays` only processes plays with
  NULL norm fields, so plays normalized before the fix retained their broken
  `norm_key_core`. On subsequent scrape runs, `seed_new_canonicals` saw plays with no
  matching canonical and created ghost canonicals (0 mapped plays, but visible in
  exported CSVs).
- DB: deleted 4 ghost canonicals created by the above mechanism:
  - 2826 The Clash "Rudie Can't Fail"
  - 2828 Foy Vance "Hi, I'm The Preacher's Son"
  - 2829 Maggie Rogers "That's Where I Am"
  - 2830 Ringo Starr "It's Been Too Long"
- DB: corrected `norm_key_core`, `norm_artist`, and `norm_title_core` for canonical
  2639 (The Head And The Heart - "Don't Dream It's Over"), which held U+0092 in
  its `display_title` and a split norm_key (`don t dream it s over`).
- Post-repair: 0 ghost canonicals, 0 plays with split-contraction norm_keys, 0
  plays whose `norm_key_core` differs from their mapped canonical.

---

## [1.2.2] - 2026-04-11

### Fixed
- `scraper/normalization_logic.py`: `normalize_for_key` now converts typographic
  apostrophes (U+2019 RIGHT SINGLE QUOTATION MARK, U+2018 LEFT SINGLE QUOTATION MARK)
  to straight apostrophes before removal. Previously, curly apostrophes served by the
  source website survived into the catch-all `[^0-9a-z]+` regex and became spaces --
  "I'm" normalized to "I m" instead of "Im", producing a different `norm_key_core` and
  a duplicate canonical for the same track.
- DB: merged two duplicate canonical pairs created by the above bug:
  - Foy Vance "Hi, I'm The Preacher's Son" (canonical 2018 -> 576, 3 plays moved)
  - Maggie Rogers "That's Where I Am" (canonical 1974 -> 884, 4 plays moved)
- DB: corrected `norm_key_core` and `norm_title_core` in place for two further affected
  canonicals with no twin: Ringo Starr "It's Been Too Long" (2187), The Clash "Rudie
  Can't Fail" (2222).
- DB: recalculated `play_count`, `first_play_ts`, and `last_play_ts` from
  `plays_to_canonical` for all four surviving canonicals; corrected pre-existing staleness
  (canonical 2187 showed `play_count=1` despite having 3 mapped plays).

---

## [1.2.1] - 2026-04-10

Documentation updates to reflect post-segmentation clustering values.

### Changed
- `ANALYSIS.md`: corrected all stale numbers now that all three shows are segmented:
  - Oldies tier avg release year 1987 -> 1979 (reflects segmented 10@10 values)
  - "90's at Night" era sequencing claim demoted from "tightest in the dataset" to
    "tightest among the specialty outliers" (This Just In at 0.34 and 10@10 at 0.60
    are now tighter post-segmentation)
  - "This Just In with Meg White": freshness 89% -> 100%, avg_best_year 2023 -> 2025.8;
    added era_continuity_mean_gap=0.34 observation (tightest in dataset alongside 10@10)
  - Closest repertoire pair: Coach/Peak Music (0.80) -> Jimmy Fink/Peak Music (0.77);
    10@10 pair 0.77 -> 0.73; This Just In similarity range 0.10-0.23 -> 0.10-0.20
- `README.md`: added show clustering bullet to Analytics section; added closing pointer
  to ANALYSIS.md and outputs directory

---

## [1.2.0] - 2026-04-10

Extended segmentation to "This Just In with Meg White" and refactored segmentation
parameters to a per-show config dict.

### Added
- `analytics/era_continuity.py`: `SEGMENT_PARAMS` dict replacing the three global
  constants (`SEGMENT_BAND`, `SEGMENT_MIN_INBAND`, `SEGMENT_CONSECUTIVE_OOB`). Maps
  show name -> (band, min_inband, consec_oob); shows not listed fall back to "default".
  Architecture supports per-show tuning when future shows need different parameters.
- `analytics/era_continuity.py`: `_show_params(show)` helper that returns the param
  tuple for a given show, defaulting to `SEGMENT_PARAMS["default"]`.

### Changed
- `analytics/era_continuity.py`: "This Just In with Meg White" added to `SEGMENT_SHOWS`.
  Exploratory analysis (43 blocks) confirmed the default parameters work correctly:
  modal era lands at ~2025, the throwback tail (1-2 tracks at :50-:59, pre-2020) is
  cleanly excluded, and all 43 blocks produce valid segments.
- `analytics/era_continuity.py`: `TRACKS_SQL_10AT10` renamed to `_TRACKS_SQL_TEMPLATE`;
  `load_10at10_tracks()` renamed to `load_segmented_tracks()`. The WHERE clause is now
  built dynamically from `SEGMENT_SHOWS` via a parameterized query -- adding a show to
  `SEGMENT_SHOWS` is sufficient to include it in the load.
- `analytics/era_continuity.py`: `_segment_block`, `get_inband_tracks`, and
  `compute_segmented_metrics` now receive per-show params via `_show_params()` in
  their groupby loops rather than reading global constants directly.
- `analytics/era_continuity.py`: segmented metrics CSV renamed from
  `era_continuity_10at10_segmented.csv` to `era_continuity_segmented.csv`.
- `analytics/show_clustering.py`: import updated from `load_10at10_tracks` to
  `load_segmented_tracks`; `tracks_10` variable renamed to `tracks_seg`.

### Result
- "This Just In with Meg White" clustering features now reflect in-band tracks only:
  `avg_best_year` 2023.2 -> 2025.8, `freshness_pct` 0.901 -> 1.000,
  `era_continuity_mean_gap` 3.40 -> 0.34.
- Era continuity charts: show labelled "This Just In with Meg White *"; mean gap
  0.34 yr (was 3.4 yr including throwback tail); era break rate 0.0% (was 11%).
- "90's at Night" examined and explicitly not added: 96.4% of plays already in-era,
  7/193 OOB tracks scattered (not systematic bleed). Documented in FUTURE_DIRECTIONS.md.

---

## [1.1.0] - 2026-04-10

Segmentation propagated to all charts and show clustering.

### Added
- `analytics/era_continuity.py`: `get_inband_tracks(tracks_df)` -- public function that
  applies density-based segmentation per (show, date, hour) block and returns only in-band
  rows, preserving all columns. Mirrors `_segment_block` logic but tracks DataFrame indices
  instead of year values so the full row survives the filter.
- `analytics/era_continuity.py`: `TRACKS_SQL_10AT10` now selects `play_id`, `canonical_id`,
  and `norm_artist` (needed by show_clustering to splice filtered rows back into its plays df).
- `analytics/show_clustering.py`: `_display_label(show)` helper returns `"<show> *"` for
  `SEGMENT_SHOWS`, plain name otherwise. Applied to all dendrogram label lists and heatmap axes.

### Changed
- `analytics/era_continuity.py`: `compute_segmented_metrics` now collects `mid_era` per pair
  and adds `mid_pct` and `avg_era` to the metrics output (required by `chart_fingerprint` and
  `chart_buckets`).
- `analytics/era_continuity.py`: `run_era_continuity` now builds a `display_df` after
  segmentation -- SEGMENT_SHOWS rows replaced with segmented values and asterisk labels --
  and passes it to all three chart functions. The raw `df` is still used for CSV export and
  the terminal comparison table (no asterisks in the CSV).
- `analytics/era_continuity.py`: all three charts (`chart_mean_gap`, `chart_fingerprint`,
  `chart_buckets`) now include a `"* = density-segmented pairs"` annotation.
- `analytics/show_clustering.py`: `run_show_clustering` now replaces SEGMENT_SHOWS rows in
  the plays dataframe with in-band filtered rows before computing scalar features. This
  corrects `avg_best_year`, `freshness_pct`, and `artist_entropy` for 10@10 shows (~1979
  avg era, 0% freshness -- correct; was inflated by bleed tracks from current rotation).
- `analytics/show_clustering.py`: `era_continuity_mean_gap` for SEGMENT_SHOWS is explicitly
  overridden after `compute_scalar_features`, since that function's inline SQL queries the
  raw DB and cannot see the filtered dataframe.
- `analytics/show_clustering.py`: all clustering charts have `"* = density-segmented pairs"`
  annotation added.

### Result
- Era continuity charts: "10 @ 10 *" shows ~0.6 yr mean gap (was ~8.7); "10 @ 10 Weekend
  Replay *" shows ~0.6 yr (was ~9.1). Era break rate drops from ~22% to 0%.
- Show clustering features: 10@10 avg_best_year now ~1979 (was distorted by bleed tracks);
  freshness_pct now 0.000; era_continuity_mean_gap now 0.60 (was ~8.7).

---

## [1.0.0] - 2026-04-10

Phase Two complete. All year accuracy work (Stages 1-5) and show clustering are now in.

### Added
- `analytics/show_clustering.py`: four-pass hierarchical show clustering (scalar features,
  repertoire similarity, combined). Three-cluster structure confirmed stable across all passes.
  `python rs_main.py cluster` entry point. See `ANALYSIS.md` for findings.
- `analytics/era_continuity.py`: density-based 10@10 segment detection. Each hour block gets
  a modal era (±3 yr density window); two consecutive OOB tracks signal segment end; 8-track
  minimum for a valid segment. Bleed tracks excluded from pair computation at query time.
  Result: 10@10 continuity 75% -> 99%, era breaks 22% -> 0% after filtering.
  Segmented metrics saved to `era_continuity_10at10_segmented.csv`.
- `scraper/db.py`: `mb_title_artist_year`, `mb_ta_status`, `manual_year_override` columns
  added to `canonical_tracks` (idempotent migration).
- `scraper/mb_enrichment.py`: second pass -- title/artist search against MB recording endpoint
  with release-group type filtering (studio Album/Single only). Results stored separately
  from ISRC pass. Both passes run on all `spotify_status = 'SUCCESS'` tracks.
- 12 `manual_year_override` corrections applied for tracks that automated enrichment could
  not resolve (Beatles, Moody Blues, Eric Burdon, Spencer Davis Group, et al.).
- All year-dependent analytics: enrichment guard (`spotify_status = 'SUCCESS'` AND both MB
  status columns NOT NULL) ensures only fully-enriched tracks contribute to year metrics.

### Fixed
- `scraper/mb_enrichment.py`: network-level failures (SSL reset) now retry 3x with 5s/10s
  backoff; `timeout=30` on all MB API calls.
- `scraper/mb_enrichment.py`: `_integrity_check()` runs at all exit points; warns on any
  SUCCESS status / NULL year mismatches.
- `scraper/mb_enrichment.py`: Lucene special characters in artist/title now quoted to prevent
  400 errors. ISRCs uppercased before lookup (Spotify stores some lowercase).

### Changed
- `best_year` resolution updated to use both MB sources:
  `manual_year_override > min(mb_isrc_year, mb_title_artist_year if < Spotify) > spotify_album_release_year`
- `analytics/analysis.py`: `average_album_year_by_show` and `freshness_percentage_by_show`
  now receive the fully-enriched subset (`df_year`); structural metrics unchanged.

---

## [0.9.1] - 2026-04-02

### Changed
- `analytics/heatmap_weekly_density.py`: sparse cells (fewer than 3 date observations)
  are now nulled out and masked from the colour scale rather than filled with 0.
  Previously, near-zero Sunday slots anchored the scale floor and compressed the
  contrast across all other cells. Behaviour now matches `heatmap_avg_release_year.py`.

---

## [0.9.0] - 2026-03-29

### Phase Two: Release Year Accuracy (see PHASE_TWO.md)

**Completed:**
- Stage 1 (schema): `spotify_album_type`, `spotify_isrc`, `mb_first_release_year`,
  `mb_lookup_status`, `mb_looked_up_at` added to `canonical_tracks` via idempotent
  migration in `scraper/db.py`
- Stage 3 (MusicBrainz lookup): `scraper/mb_enrichment.py` queries MB by ISRC for
  compilation-matched and remaster-flagged tracks; rate-limited at 1.1s/req with
  User-Agent header per MB requirements
- Stage 4 (analytics): all year-dependent analytics switch from `spotify_album_release_year`
  to `best_year` CASE expression; MB year accepted only when strictly earlier than
  Spotify's (handles remaster ISRC false positives -- validated: Bowie "Fame" stays
  at 1975, Clash "I Fought The Law" corrected 1979, Allman Brothers "Jessica" corrected 1973)

**In progress -- ~1-2 runs remaining as of 2026-03-31 (1,832 of 2,591 done, 759 remaining):**
- Stage 2 (Spotify backfill): `scraper/spotify_backfill.py` backfills `spotify_album_type`
  and `spotify_isrc` for existing SUCCESS records; rate-limited to ~600/day.
  `scraper/enrichment.py` updated to populate both fields on all new enrichments.
  Run sequence: `python rs_main.py enrich-meta` then `python rs_main.py mb-enrich`

**Key finding from 600-track sample:**
- 8.8% compilation, 16.0% album-type with remaster/deluxe signals -> ~24.8% year-suspect
  (higher than anticipated; remaster heuristic is necessary, not optional)
- Remaster heuristic signals: `remaster`, `deluxe`, `anniversary`, `expanded`, `edition`

### Added
- `analytics/era_continuity.py`: three consecutive-pair release year metrics per show
  (era continuity %, mean absolute year gap, era break rate), three Plotly charts,
  CSV output. Thresholds configurable at top of file.
- `scraper/spotify_backfill.py`: one-time backfill of ISRC and album_type for existing
  SUCCESS records; idempotent, skips already-populated records
- `scraper/mb_enrichment.py`: MusicBrainz ISRC lookup for compilation/remaster tracks;
  stores `mb_first_release_year` and `mb_lookup_status`; idempotent

## [0.8.0] - 2026-03-24

### Added
- `analytics/wednesday_freshness.py`: day-of-week freshness analysis testing whether
  Wednesday programming has a measurably higher bias toward recently-released tracks.
  Motivated by 107.1 The Peak's public programming claim that Wednesdays feature at
  least one new song per broadcast hour.
  - "New" defined as a rolling window relative to play date (not absolute), so the
    analysis stays valid as the dataset ages
  - 14-day forward buffer applied to account for confirmed advance/promo plays
    (13 pre-release plays observed in dataset; max gap 10 days)
  - Two metrics: % of plays qualifying as new, and % of broadcast hours containing
    at least one new track (the latter directly tests the station's programming claim)
  - Three thresholds tested: 8 weeks, 16 weeks, 24 weeks
  - Run both including and excluding format-biased shows (This Just In with Meg White,
    10 @ 10, 10 @ 10 Weekend Replay, 90's at Night)
  - Output: `analytics/outputs/wednesday_freshness.html` (Plotly interactive)
- `analytics/wednesday_freshness.py` wired into `rs_main.py analyze` via `run_analysis()`
- `CLAUDE.md` added to project root with architecture, DB schema, enrichment behavior,
  entry points, and data integrity rules for onboarding future sessions

### Changed
- `README.md`: dataset stats updated to reflect current size (43 days, >12,000 plays)

### Findings
- Wednesday shows a modest freshness edge at tight thresholds (8w, 16w) -- roughly
  1-2 percentage points above the next highest day -- consistent with the station's
  claimed programming policy, but not dramatically distinct from other days
- At the 24-week threshold the edge disappears, suggesting the signal is specific to
  genuinely recent releases rather than a broad catalog skew
- Excluding format shows does not materially change the pattern; the Wednesday signal
  is present in the general rotation, not driven by a single show
- With ~6 weeks of each weekday in the current dataset the result is suggestive but
  not conclusive; interpretation should be framed as "consistent with the claim"
  rather than as a confirmed finding

### Future analysis
- Once the dataset covers several months, a show-level breakdown of Wednesday freshness
  will be meaningful: currently the day-level signal is visible but it is not possible
  to determine whether it is spread across the full Wednesday rotation or concentrated
  in one show's scheduling slot

## [0.7.0] - 2026-03-23

### Fixed
- `enrichment.py`: `enriched_this_run` counter was incremented when an override
  entry was *found*, before the Spotify fetch confirmed 200 — so `enriched=12`
  could be logged even when all 12 override fetches returned 404. Counter now
  incremented only after a successful fetch inside `if selected:`
- `enrichment.py`: override fetch failures were silently `continue`d with no
  visibility; now tracked via `override_failure_count` and returned in the
  enrichment summary

### Added
- `enrichment.py`: `override_failures` counter returned in enrichment summary
- `weekly.py`: `override_failures` wired into weekly summary; ATTENTION warning
  logged when any override fetches fail (prompts check of manual_spotify_overrides
  for bad IDs)

## [0.6.0] - 2026-03-23

### Fixed
- `enrichment.py`: critical indentation bug — `if selected:` (SUCCESS write) was
  nested inside `if not selected:`, meaning no manual override has ever
  successfully enriched a track since the mechanism was written
- `enrichment_failures.csv`: was including NON_MUSIC and would have included
  NO_MATCH; now filtered to FAILED only (actionable items)

### Added
- `NO_MATCH` status: for canonicals confirmed as unresolvable on Spotify;
  excluded from enrichment API calls, failures report, and audit warnings
- `enrichment_failures.csv`: added `last_play_ts` column (most recent play
  date for the canonical) to help prioritise which failures to investigate
- `enrichment_attempt_3_4.csv`: added `spotify_track_name` and
  `spotify_primary_artist_name` columns
- `audit.py`: unenriched section now distinguishes actionable (FAILED) from
  closed (NO_MATCH, NON_MUSIC); only actionable items trigger a warning

## [0.5.0] - 2026-03-22

### Fixed
- `enrichment.py`: PENDING records with attempt_count > 0 (legacy residue from
  schema migration) corrected to FAILED; auto-repair guard now runs at the start
  of every enrichment run to prevent recurrence
- `audit.py`: emoji in terminal print was crashing on Windows (cp1252 encoding);
  replaced with plain ASCII

### Added
- `config.py`: `LOW_PLAY_SUPPRESSED_SHOWS` and `LOW_PLAY_SUPPRESSED_TITLE_SIGNALS`
  constants for configurable warning suppression
- `orchestrator.py`: low play count warnings now suppressed for known non-music
  shows and hours containing specified title signals; suppressed hours logged at
  INFO instead of WARNING
- `enrichment.py`: `new_failures` counter tracks first-attempt failures separately;
  returned in enrichment summary
- `weekly.py`: `new_failures` surfaced in weekly log; ATTENTION warning emitted
  when any first-time failures occur
- `audit.py`: reports all non-SUCCESS canonicals grouped by status on every run

## [0.4.1] - 2026-03-20

### Fixed
- Corrected `spotify_album_release_year` for Peter Gabriel - "Shaking The Tree"
  from 1900 to 1990 (Spotify data entry error)
- `scraper/enrichment.py`: added plausibility guard — release years outside
  1920 to current_year+1 are logged as warnings and nulled out rather than stored
- `analytics/boxplot_release_year.py`: added year >= 1920 floor filter as a
  safety net against bad upstream data skewing the y-axis

## [0.4.0] - 2026-03-20

### Added
- `analytics/analysis.py`: `top_fresh_tracks_by_week()` — for each ISO week,
  returns top 5 most-played tracks with a Spotify release date within the last
  12 months (rolling window, not calendar year)
- `analytics/analysis.py`: `print_fresh_tracks_report()` — formatted terminal
  output of the weekly fresh tracks results
- `analytics/outputs/analytics_fresh_tracks.csv` — persisted weekly fresh
  tracks report, written on each `analyze` run

## [0.3.0] - 2026-03-20

### Added
- `analytics/analysis.py`: `artist_breadth()` — global count of distinct songs
  played per artist, with total plays, repeat ratio, and show count
- `analytics/analysis.py`: structured logging with 5-log rotation; all metrics
  now logged to `logs/analysis_*.log`
- `analytics/analysis.py`: `run_analysis()` entry point callable from rs_main
- `analytics/visuals.py`: `run_visuals()` — runs scatter plot and box plot together
- `rs_main.py`: `analyze` mode now fully wired; runs all metrics, exports CSVs,
  and generates all visuals in one command

### Changed
- `analytics/visuals.py`: removed `plt.show()` so scatter plot saves to file
  without blocking on an interactive window

## [0.2.0] - 2026-03-20

### Added
- `analytics/boxplot_release_year.py`: interactive Plotly box plot showing
  distribution of track release years per show, sorted by median year

### Fixed
- `requirements.txt`: was saved in UTF-16 encoding, breaking pip installs
  from a fresh clone; re-saved as UTF-8 and added plotly>=6.6

## [0.1.0] - 2026-03-20

### Fixed
- `enrichment.py`: manual override path was using stale similarity scores from
  the previous loop iteration; scores now reset to None at the start of each track
- `enrichment.py`: `rate_limit_abort` flag was never propagating back to the
  caller; weekly pipeline was always logging `abort=False` even on true aborts
- `audit.py`: broken import (`from utils import rotate_logs`) would crash the
  audit mode at startup; fixed to `from scraper.utils import rotate_logs`
- `audit.py`: hardcoded `"radio_plays.db"` path replaced with `DB_PATH` from config

### Removed
- `rs_main.py`: ~130 lines of dead and duplicate code (three functions defined
  twice each, a nested function that was defined but never called, and unreachable
  code after a return statement)
- `parsing.py`: duplicate import block mid-file
- `normalization_logic.py`: stale dated dev note on line 1
- `audit.py`: unused imports (`MIN_PLAYS_PER_HOUR`, `MAX_PLAYS_PER_HOUR`)

### Changed
- `rs_main.py`: `run_full_audit()` now called at the end of the scrape pipeline,
  so post-pipeline data quality checks actually run

---

## [Baseline] - 2026-03-20

Establishing changelog. Project was already in operation at this point.

### In place at baseline
- Daily scrape pipeline (hourly playlist ingestion via Windows Task Scheduler)
- Weekly enrichment pipeline (Spotify Web API metadata, RapidFuzz similarity scoring)
- SQLite database with plays, canonical_tracks, and plays_to_canonical tables
- Normalization and canonicalization of artist/title strings
- Analytics layer: Shannon entropy, unique artists per hour, freshness %, avg release year
- Heatmap visualizations: weekly density, average release year by show
- Scatter plot: programming density vs. contemporary bias
- Structured logging with log rotation and anomaly flagging
- Idempotent ingestion with unique play constraint
- Spotify credential management via .env
