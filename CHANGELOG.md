# Changelog

All notable changes to this project will be documented here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

---

## [Unreleased]

### Planned
- Audit system hardening: fix known bugs, consolidate anomaly detection logic
- Artist breadth metric: unique song count per artist across all plays
- Release year box plot: per-show distribution of track release years
- Weekly fresh tracks report: top 5 recently released songs by play count per week

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
