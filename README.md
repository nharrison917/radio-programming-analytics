# Radio Programming Analytics Pipeline

## Overview

This project builds an end-to-end data pipeline that scrapes radio playlist data, normalizes and resolves track metadata, enriches tracks using Spotify’s Web API, and performs structured programming analysis.

The goal was to demonstrate how raw web data can be transformed into a structured database and leveraged for defensible programming analytics.

The project includes:

- Automated playlist scraping
- Canonical track resolution (many plays → one canonical track)
- Spotify API enrichment with retry logic and rate-limit handling
- SQLite database design with structured logging
- Analytical metrics evaluating programming structure and catalog bias

---

## Dataset

Current dataset:
- ~1 month of scraped playlist data
- Thousands of plays
- Fully mapped to canonical tracks
- ~98% Spotify enrichment match rate

Analysis is descriptive and limited to the current dataset size.

---

## Architecture

Pipeline stages:

1. **Ingestion**
   - Scrapes hourly playlist pages
   - Deduplicates using unique play timestamp constraint
   - Logs anomalies and ingestion metrics

2. **Canonicalization**
   - Normalizes artist/title strings
   - Maps multiple plays to a canonical track
   - Tracks confidence per play mapping

3. **Enrichment**
   - Spotify Web API integration
   - Multi-stage search fallback logic
   - Token similarity scoring (RapidFuzz)
   - Rate-limit detection and cooldown handling
   - Manual override support

4. **Analytics Layer**
   - Artist diversity (Shannon entropy)
   - Unique artists per broadcast hour (normalized)
   - Exclusive artist percentage
   - Average album release year
   - Freshness (% of tracks released in last 5 years)
   - Programming density vs. contemporary bias visualization

---

## Key Insights

Even with one month of data, structural differences between shows emerge:

- Some shows emphasize high diversity per broadcast hour.
- Others maintain tighter rotation patterns.
- Certain programs strongly skew toward contemporary releases.
- Era-defined programming is clearly visible in release-year metrics.

The visualization below maps programming density against contemporary bias:

![Programming Density vs Freshness](analytics/outputs/density_vs_freshness.png)

---

## Example Visualization

`analytics/outputs/density_vs_freshness.png`

This scatter plot shows:

- X-axis: Unique artists per broadcast hour
- Y-axis: % of tracks released within last 5 years
- Color: Freshness intensity

Outliers highlight distinct programming strategies.

---

## Tech Stack

- Python
- SQLite
- pandas
- BeautifulSoup
- requests
- RapidFuzz
- Spotify Web API
- matplotlib
- VS Code

---

## How to Run

1. Clone the repository
2. Create and activate a virtual environment
3. Install dependencies:
pip install -r requirements.txt
4. Run scraper:
python rs_main.py
5. Run analytics:
python analytics/analysis.py
6. Generate visualization:
python analytics/visuals.py


---

## Design Philosophy

- Idempotent ingestion
- Explicit state tracking
- Clear separation of ingestion, normalization, enrichment, and analytics
- Minimal silent failure
- Structured logging and observability

---

## Future Extensions

As the dataset grows:

- Time-series trend analysis
- Rotation velocity modeling
- Programming archetype clustering
- Extended Spotify audio feature analysis

---

## Author

Independent quantitative analytics portfolio project.