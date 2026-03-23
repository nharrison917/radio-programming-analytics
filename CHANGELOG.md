# Changelog

All notable changes to this project will be documented here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

Development assisted by Claude Code (Anthropic).

---

## [Unreleased]

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
