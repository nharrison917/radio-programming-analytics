# Radio Scraper Project – Current State

## Goal
Build a reproducible data pipeline that:
Scrapes historical radio playlist data
Cleans and normalizes track metadata
Resolves plays to canonical tracks
Enriches tracks using Spotify’s Web API
Stores structured data for downstream music analytics

The project demonstrates incremental data ingestion, entity resolution, API integration, retry logic, rate-limit handling, and operational logging.

## Current Status
### Data Ingestion
Scrapes hourly playlist data from radio station website

Handles:
Time parsing
Station show extraction
Encoding repairs
Smart punctuation normalization
Enforces deduplication via unique index on (play_ts)
Tracks confidence score per play
Respects robots.txt and crawl-delay (8 seconds)

### Data Storage
SQLite database with structured tables:
plays
canonical_tracks
plays_to_canonical
manual_spotify_overrides
play_insert_conflicts

Automatic database backups:
Before scrape
Before weekly pipeline

Structured logging:
File + console
Daily scrape summary
Weekly enrichment summary
Metrics logging
Anomaly detection

## Canonicalization Layer

Normalizes:
Titles
Artists
Feature tags
Version notes

Extracts:
Core title
Full normalized title
Version type
Version year
Maps many plays → one canonical track
Tracks per-play audit confidence

Current scale:
~5,000+ plays
~1,800 canonicals
Fully mapped

## Enrichment Layer
Integrates Spotify Web API
Multi-attempt search logic (4-stage fallback)
Token-set similarity scoring (RapidFuzz)
Threshold-controlled matching (90/85)
Manual override table for edge cases
Rate-limit detection with dynamic resume time
Chunked processing with cooldown
48-hour retry spacing to prevent hammering

Attempt tracking:
spotify_attempt_count
spotify_last_attempted_at
Permanent failure classification:
NON_MUSIC
FAILED_PERMANENT

Current enrichment performance:
~98% match rate on attempt 1
Very low fallback rate

Remaining unresolved tracks primarily:
Covers not in Spotify
Non-music content
Radio-specific versions

## Operational Observability
###  Daily Scrape Logging
Scrape window (start/end)
Pages attempted/fetched
Plays seen/inserted
Hour-level anomaly detection
Suspicious title detection
Null station_show detection
Database snapshot metrics

### Weekly Enrichment Logging
Canonicals processed
Enriched count
Failure count
Attempt distribution
Rate-limit abort detection
Resume time calculation
Full-database audit mode implemented.

## Known Limitations
Dependent on radio website structure stability
Covers or non-catalog tracks cannot be enriched automatically
Spotify rate limits restrict batch throughput

No automated scheduler yet (manual execution)

## Planned Enhancements

Move enrichment to scheduled weekly task
Improve Spotify resume logic with retry caps
Add enrichment failure categorization dashboard
Expand analytics layer (rotation analysis, show-based trends)
Integrate limited Spotify audio features (if API allows)
Improve metadata anomaly classification

## Tech Stack

Python
requests
BeautifulSoup
ftfy
pandas
sqlite3
rapidfuzz
Spotify Web API
logging
VS Code (modular structure)

## Folder Structure
radio_scraper/
│
├── rs_main.py
├── scraper/
│   ├── ingestion.py
│   ├── parsing.py
│   ├── normalization_logic.py
│   ├── enrichment.py
│   ├── weekly.py
│   ├── audit.py
│   └── config.py
├── logs/
├── backups/
└── radio_plays.db



## Design Philosophy
Idempotent ingestion
Explicit state tracking
Manual override safety layer
Structured logging
Operational resilience
Minimal silent failure
Clean separation of ingestion, normalization, enrichment, and analytics