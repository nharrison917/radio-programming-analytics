# CLAUDE.md - Radio Programming Analytics Pipeline

## What this project is

An end-to-end data pipeline scraping radio playlist data from 107.1 The Peak,
enriching tracks via the Spotify Web API, and producing structured programming
analytics. Portfolio/learning project. Dataset: 67 days, >19,000 plays, ~98.4%
Spotify match rate (as of 2026-04-17).

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
   `analytics/era_continuity.py`, `analytics/wednesday_freshness.py`,
   `analytics/show_clustering.py`, `analytics/band_age.py`,
   `analytics/primary_artist_mismatch.py`
   Shannon entropy, freshness %, avg release year, artist breadth, weekly fresh tracks,
   era continuity metrics, hierarchical show clustering, band age at recording,
   primary artist mismatch quality report.
   All outputs generated via `python rs_main.py analyze`.

## Entry points

```bash
python rs_main.py scrape      # Daily ingestion (also runs audit)
python rs_main.py weekly      # Enrichment run
python rs_main.py analyze     # All analytics + visuals
python rs_main.py cluster     # Show clustering analysis
python rs_main.py audit       # Standalone audit
python rs_main.py enrich-meta # Backfill spotify_isrc + spotify_album_type (~600/day limit)
python rs_main.py mb-enrich         # MusicBrainz ISRC lookup for compilation/remaster tracks
python rs_main.py mb-artist-enrich  # MusicBrainz artist MBID + earliest release year backfill
python rs_main.py add-override --id <canonical_id> --spotify-id <spotify_id>  # Spotify ID override for FAILED tracks (see MANUAL_OVERRIDE.md for wrong SUCCESS-match procedure)
python rs_main.py set-meta --id <canonical_id> [--year YYYY|YYYY-MM-DD] [--duration M:SS]  # Manual year/duration for non-Spotify tracks
python rs_main.py backfill --start YYYY-MM-DDTHH:MM --end YYYY-MM-DDTHH:MM
```

Scheduled via Windows Task Scheduler using `run_scraper.bat` / `run_weekly.bat`.

The `scrape` mode runs five steps in sequence:
ingest -> normalize -> seed canonicals -> map plays -> audit

## Database schema

| Table | Purpose | Key columns |
|---|---|---|
| `plays` | Raw play records | `id`, `play_ts`, `station_show`, `is_music_show`, `title`, `artist`, `norm_key_core` |
| `canonical_tracks` | Deduplicated track entities with Spotify metadata | `canonical_id`, `norm_key_core`, `display_artist`, `display_title`, `play_count`, `first_play_ts`, `last_play_ts`, `spotify_id`, `spotify_status`, `spotify_album_release_year`, `spotify_album_type`, `spotify_isrc`, `spotify_primary_artist_name`, `spotify_primary_artist_id`, `mb_isrc_year`, `mb_lookup_status`, `mb_title_artist_year`, `mb_ta_status`, `manual_year_override`, `manual_release_date`, `manual_duration_ms` |
| `canonical_artists` | Per-artist Spotify metadata (career-level) | `spotify_artist_id`, `artist_name`, `earliest_release_year`, `enrichment_status`, `mb_artist_id`, `mb_earliest_release_year`, `mb_artist_status` |
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
| `scraper/mb_artist_enrichment.py` | MusicBrainz artist MBID resolution + earliest release year (band age metric) |
| `scraper/overrides.py` | Manual override CLI: `add-override` (Spotify ID) and `set-meta` (year/duration) |
| `scraper/audit.py` | Post-pipeline data quality checks |
| `analytics/era_continuity.py` | Consecutive-pair era metrics; `get_inband_tracks()` for density-based segmentation |
| `analytics/show_clustering.py` | Four-pass hierarchical show clustering (scalar, repertoire, combined, equal-weight) |
| `radio_plays.db` | SQLite database (not in repo -- generated at runtime) |
| `analytics/outputs/quality_checks/enrichment_failures.csv` | FAILED-status canonicals only (actionable) |
| `.env` | Spotify credentials + scraper contact (not in repo) |

## Enrichment status model

- `PENDING` -- not yet attempted
- `SUCCESS` -- matched on Spotify
- `FAILED` -- attempted, no match found (appears in failures CSV)
- `NO_MATCH` -- closed; excluded from API calls and warnings. Set manually, or
  automatically by `set-meta --year` when applied to a FAILED track (year is
  authoritative, no Spotify retry needed)
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

`mb-enrich` now runs two passes: ISRC lookup then title/artist search, both against
`spotify_status = 'SUCCESS'` tracks where `manual_year_override IS NULL`. Tracks with
a manual year override are skipped — that year is authoritative and further lookups are
wasted work. Both statuses are tracked independently: `mb_lookup_status` for ISRC,
`mb_ta_status` for title/artist.

## Outputs

`analytics/outputs/` is organised into subdirectories by concern. Each analytics script
defines its own `*_DIR` constant derived from `OUTPUT_DIR`. Only `quality_checks/` is
tracked in git -- all other subdirectories are gitignored.

| Subdirectory | Contents | Git |
|---|---|---|
| `quality_checks/` | enrichment_failures.csv, enrichment_attempt_3_4.csv, spotify_failed.csv, mb_failed.csv, segment_breakers.csv, mb_artist_missing.csv, mb_artist_large_delta.csv, band_age_negative.csv, band_age_extreme.csv, primary_artist_mismatch.csv | Tracked |
| `clustering/` | cluster_*.html, show_clustering_features.csv | Ignored |
| `era/` | boxplot_release_year.html, heatmap_avg_release_year.png, analytics_avg_album_year.csv | Ignored |
| `era_continuity/` | era_continuity*.csv, era_continuity*.html | Ignored |
| `freshness/` | wednesday_freshness.html, density_vs_freshness.png, analytics_freshness.csv, analytics_fresh_tracks.csv | Ignored |
| `rotation/` | heatmap_weekly_density.png, analytics_entropy.csv, analytics_exclusive_artists.csv, analytics_artist_breadth.csv, analytics_unique_artists_per_hour.csv | Ignored |
| `band_age/` | boxplot_band_age.html, band_age_summary.csv | Ignored |

HTML outputs use Plotly (interactive); static charts use matplotlib.
- `logs/` -- rotating logs: `scrape_*.log`, `weekly_*.log`, `audit_*.log`, `analysis_*.log`
- `backups/` -- timestamped DB snapshots (not in repo)

## Suppressed warnings

Configured in `config.py`:
- `LOW_PLAY_SUPPRESSED_SHOWS` -- shows known to play little/no music
- `LOW_PLAY_SUPPRESSED_TITLE_SIGNALS` -- title strings that suppress warnings for
  that hour and the following hour

## What's in scope right now (as of 2026-04-16)

- **Show clustering (v1.7.4):** scalar pass uses six features -- `median_best_year`,
  `exclusive_artist_pct`, `era_continuity_mean_gap`, `era_spread`, `rotation_depth`,
  `band_age_score` (composite). Repertoire pass uses TF-IDF cosine similarity on full
  artist + track vocabulary (replaced binary top-10/top-20). See PLAN.md for cluster
  assignments and rationale.
- MB artist enrichment: 99.2% coverage. Remaining open items in `mb_artist_missing.csv`
  and `mb_artist_large_delta.csv`. Known wrong-entity cases: Monotones, Tom Hamilton.
- band_age quality reports: `band_age_negative.csv` (band_age < -2) and
  `band_age_extreme.csv` (band_age > 50) generated by `band_age.py` each analyze run.
  Negative band_age is a signal for wrong Spotify primary artist or bad MB data.
- `primary_artist_mismatch.csv`: flags SUCCESS tracks where `display_artist` does not
  closely match `spotify_primary_artist_name` (catches collab/cover wrong-version matches
  that score 100/100 in enrichment but store the wrong primary artist). Generated by
  `analytics/primary_artist_mismatch.py` each analyze run. Threshold=75 in that file.
- Known enrichment bug: `add-override` silently ignored for SUCCESS tracks (enrichment
  WHERE clause only selects PENDING/FAILED). Workaround in MANUAL_OVERRIDE.md. Proper
  fix: expand WHERE to include tracks in `manual_spotify_overrides`.
- Phase Three: MBID-based manual year overrides (see PHASE_THREE.md) -- not yet started
- Dataset growth and continued enrichment runs

## Segmented shows

Some shows require density-based segmentation before their plays can be used in
show-level analytics. The canonical list is `SEGMENT_SHOWS` in `era_continuity.py`:

```python
SEGMENT_SHOWS = (
    "10 @ 10",
    "10 @ 10 Weekend Replay",
    "This Just In with Meg White",
)
```

These shows have a known structure where a subset of each hour block is analytically
distinct from the surrounding plays. For 10@10, bleed tracks from the station's
regular rotation surround a single-era themed segment. For "This Just In", a new-music
main block is followed by a 1-2 track intentional throwback tail at :50-:59. In both
cases the surrounding/tail tracks are noise for show-identity analytics and distort
every scalar feature (median_best_year, era_spread, era_continuity_mean_gap, etc.).

Per-show segmentation parameters live in `SEGMENT_PARAMS` in `era_continuity.py`.
All three current shows use the default (band=3yr, min_inband=8, consec_oob=2).

**To add a new segmented show:**
1. Add the show name to `SEGMENT_SHOWS` -- cascades to SQL, chart labels, and the `era_continuity_mean_gap` override in `show_clustering` automatically.
2. Add a show-specific entry to `SEGMENT_PARAMS` only if the default parameters won't work.
3. Run a per-block trace to verify expected block validity fraction before committing.

**Rule:** Any show-level analysis that characterises programming style (era continuity
charts, show clustering, etc.) must filter SEGMENT_SHOWS to in-band tracks only via
`get_inband_tracks()` in `era_continuity.py`. Segmented shows are labelled `<name> *`
in all charts, with annotation `"* = density-segmented pairs"`.

Raw play data is never deleted; segmentation is applied at query/analysis time only.

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
