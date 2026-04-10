# CLAUDE.md - Radio Programming Analytics Pipeline

## What this project is

An end-to-end data pipeline scraping radio playlist data from 107.1 The Peak,
enriching tracks via the Spotify Web API, and producing structured programming
analytics. Portfolio/learning project. Dataset: 50 days, >14,000 plays, ~98.7%
Spotify match rate (as of 2026-03-31).

## Pipeline stages

1. **Ingestion** -- `scraper/fetch.py`, `scraper/parsing.py`, `scraper/orchestrator.py`
   Scrapes hourly playlist pages. Idempotent (unique play constraint).
2. **Canonicalization** -- `scraper/canonical.py`, `scraper/normalization.py`, `scraper/normalization_logic.py`
   Normalizes artist/title strings, maps plays to canonical track entities.
3. **Enrichment** -- `scraper/enrichment.py`, `scraper/weekly.py`
   Spotify Web API integration. Multi-stage fallback, RapidFuzz similarity scoring,
   manual override via `manual_spotify_overrides`. Override IDs must be verified --
   bad IDs log an ATTENTION warning via `override_failures` counter.
4. **Analytics** -- `analytics/analysis.py`, `analytics/visuals.py`,
   `analytics/boxplot_release_year.py`, `analytics/heatmap_*.py`,
   `analytics/era_continuity.py`, `analytics/wednesday_freshness.py`
   Shannon entropy, freshness %, avg release year, artist breadth, weekly fresh tracks,
   era continuity metrics. All outputs generated via `python rs_main.py analyze`.

## Entry points

```bash
python rs_main.py scrape      # Daily ingestion (also runs audit)
python rs_main.py weekly      # Enrichment run
python rs_main.py analyze     # All analytics + visuals
python rs_main.py audit       # Standalone audit
python rs_main.py enrich-meta # Backfill spotify_isrc + spotify_album_type (~600/day limit)
python rs_main.py mb-enrich   # MusicBrainz ISRC lookup for compilation/remaster tracks
python rs_main.py backfill --start YYYY-MM-DDTHH:MM --end YYYY-MM-DDTHH:MM
```

Scheduled via Windows Task Scheduler using `run_scraper.bat` / `run_weekly.bat`.

The `scrape` mode runs five steps in sequence:
ingest -> normalize -> seed canonicals -> map plays -> audit

## Database schema

| Table | Purpose | Key columns |
|---|---|---|
| `plays` | Raw play records | `id`, `play_ts`, `station_show`, `is_music_show`, `title`, `artist`, `norm_key_core` |
| `canonical_tracks` | Deduplicated track entities with Spotify metadata | `canonical_id`, `norm_key_core`, `display_artist`, `display_title`, `play_count`, `first_play_ts`, `last_play_ts`, `spotify_id`, `spotify_status`, `spotify_album_release_year`, `spotify_album_type`, `spotify_isrc`, `spotify_primary_artist_name`, `spotify_primary_artist_id`, `mb_isrc_year`, `mb_lookup_status`, `mb_title_artist_year`, `mb_ta_status`, `manual_year_override` |
| `canonical_artists` | Per-artist Spotify metadata (career-level) | `spotify_artist_id`, `artist_name`, `earliest_release_year`, `enrichment_status` |
| `plays_to_canonical` | Many-to-one mapping of plays to canonicals | `play_id`, `canonical_id`, `match_method` |
| `manual_spotify_overrides` | Hand-supplied Spotify IDs for FAILED tracks | `canonical_id`, `spotify_id` |
| `play_insert_conflicts` | Idempotency conflict log | (rarely queried directly) |

Notes: `canonical_tracks.spotify_status` uses the enrichment status model below.
`canonical_tracks` has no Python `CREATE TABLE` -- it was created directly in SQLite.

## Key files

| File | Purpose |
|---|---|
| `scraper/config.py` | All constants, thresholds, DB_PATH, credentials |
| `scraper/enrichment.py` | Spotify enrichment logic, override handling |
| `scraper/artist_enrichment.py` | Artist career metadata (earliest_release_year) via Spotify; runs as part of `weekly` |
| `scraper/spotify_backfill.py` | One-time backfill of ISRC + album_type for existing records |
| `scraper/mb_enrichment.py` | MusicBrainz ISRC lookup for compilation/remaster tracks |
| `scraper/audit.py` | Post-pipeline data quality checks |
| `analytics/era_continuity.py` | Consecutive-pair era metrics (continuity %, gap, break rate) |
| `radio_plays.db` | SQLite database (not in repo -- generated at runtime) |
| `analytics/outputs/enrichment_failures.csv` | FAILED-status canonicals only (actionable) |
| `.env` | Spotify credentials + scraper contact (not in repo) |

## Enrichment status model

- `PENDING` -- not yet attempted
- `SUCCESS` -- matched on Spotify
- `FAILED` -- attempted, no match found (appears in failures CSV)
- `NO_MATCH` -- manually confirmed unresolvable; excluded from API calls and warnings
- `NON_MUSIC` -- non-music content; excluded from enrichment and failures report

Only `FAILED` records are actionable. `NO_MATCH` and `NON_MUSIC` are closed.

## Enrichment matching behavior

- Matching uses RapidFuzz `token_set_ratio`: `TITLE_THRESHOLD = 90`, `ARTIST_THRESHOLD = 85`
- Four search attempts per track, from strict to loose (see `enrichment.py`)
- FAILED records are only retried if `spotify_last_attempted_at < 2 days ago` --
  a `weekly` run will not retry everything every time

## Data integrity rules

- Release years outside 1920-current_year+1 are nulled out at enrichment time
  (catches Spotify data entry errors like year=1900). Same floor applied in box plot.
- PENDING records with attempt_count > 0 are auto-corrected to FAILED at enrichment
  start (legacy schema migration residue).
- Validate numeric fields from Spotify against domain bounds before writing to DB.
  Log and null implausible values -- do not store silently.

### best_year resolution (Phase Two, revised)

All year-dependent analytics use `best_year`, not `spotify_album_release_year` directly.
Two MB sources are stored separately and both contribute to resolution:

- `mb_isrc_year` -- year from MusicBrainz ISRC endpoint (precise, version-specific)
- `mb_title_artist_year` -- year from MB recording text search filtered to studio
  Album/Single release-groups (broader coverage, less precise)
- `manual_year_override` -- human-verified correct year; takes unconditional priority

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

Each MB source is only accepted when strictly earlier than Spotify's year. When both
are available and both are earlier, the minimum is taken. This handles the remaster-ISRC
problem (e.g. Bowie - Fame: Spotify=1975, MB ISRC=2016 -- rule keeps 1975).

`mb-enrich` now runs two passes: ISRC lookup (all SUCCESS tracks) then title/artist
search (all SUCCESS tracks). Both statuses are tracked independently: `mb_lookup_status`
for ISRC, `mb_ta_status` for title/artist.

## Outputs

- `analytics/outputs/` -- CSVs and HTML visualizations
- `analytics/outputs/weekly_reports/enrichment_attempt_3_4.csv` -- tracks matched only on
  fallback attempts 3 or 4; worth spot-checking as borderline matches
- HTML outputs use Plotly (interactive); static scatter plot uses matplotlib
- `logs/` -- rotating logs: `scrape_*.log`, `weekly_*.log`, `audit_*.log`, `analysis_*.log`
- `backups/` -- timestamped DB snapshots (not in repo)

## Suppressed warnings

Configured in `config.py`:
- `LOW_PLAY_SUPPRESSED_SHOWS` -- shows known to play little/no music
- `LOW_PLAY_SUPPRESSED_TITLE_SIGNALS` -- title strings that suppress warnings for
  that hour and the following hour

## What's in scope right now (as of 2026-03-24)

- Bug sweep and pipeline friction reduction
- Moderate expansion of analytics
- Project is under active review

## Things to know

- `.venv` Python environment (not conda -- pure Python project)
- `radio_plays.db` is in `.gitignore`; never commit it
- `scratch/` contains ad-hoc SQL and debug scripts -- not production code
- `requirements.txt` must be saved UTF-8 (was UTF-16 once, broke pip installs)
- No emoji in terminal output -- Windows cp1252 crashes on non-ASCII in some contexts

## AI Codex Index

This project has a pre-built index in `.ai-codex/` to reduce cold-start token usage.
Read these files at the start of each session before exploring the codebase:

- `.ai-codex/lib.md`       -- all functions and classes, grouped by module
- `.ai-codex/structure.md` -- directory tree and key file descriptions
