# Changelog

All notable changes to this project will be documented here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

Development assisted by Claude Code (Anthropic).

---

## [Unreleased]

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
