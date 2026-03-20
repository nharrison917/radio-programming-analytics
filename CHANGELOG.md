# Changelog

All notable changes to this project will be documented here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

---

## [Unreleased]

### Planned
- Artist breadth metric: unique song count per artist across all plays
- Release year box plot: per-show distribution of track release years
- Weekly fresh tracks report: top 5 recently released songs by play count per week

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
