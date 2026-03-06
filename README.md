# Radio Programming Analytics Pipeline

## Overview

This project builds an end-to-end data pipeline that ingests radio
playlist data, resolves track metadata into canonical entities, enriches
tracks via the Spotify Web API, and generates structured programming
analytics.

The objective is to demonstrate how raw web data can be transformed into
a defensible, queryable database and used to evaluate programming
structure, rotation patterns, and catalog bias.

This project demonstrates applied data engineering and structured
analytics under real-world API and normalization constraints.

------------------------------------------------------------------------

## Dataset

Current dataset:

-   25 days of hourly playlist data
-   > 7,000 recorded plays
-   ~98% Spotify enrichment match rate
-   Fully mapped canonical track relationships

Analysis is descriptive and scoped to the current dataset size.

------------------------------------------------------------------------

## Architecture

The pipeline is divided into four stages:

### 1. Ingestion

-   Scrapes hourly playlist pages\
-   Enforces idempotency via unique play timestamp constraints\
-   Logs anomalies and ingestion metrics

### 2. Canonicalization

-   Normalizes artist/title strings\
-   Maps multiple plays to a canonical track entity\
-   Tracks per-play mapping confidence

### 3. Enrichment

-   Spotify Web API integration\
-   Multi-stage search fallback logic\
-   Token similarity scoring (RapidFuzz)\
-   Rate-limit detection with cooldown handling\
-   Manual override capability

### 4. Analytics Layer

Computes structured programming metrics including:

-   Artist diversity (Shannon entropy)\
-   Unique artists per broadcast hour (normalized)\
-   Exclusive artist percentage\
-   Average album release year\
-   Freshness (% of tracks released within last 5 years)\
-   Programming density vs. contemporary bias visualization

------------------------------------------------------------------------

## Key Findings

Even with one month of data, structural differences between programs
emerge:

-   Some shows maximize artist diversity per broadcast hour\
-   Others maintain tighter rotational patterns\
-   Certain programs skew strongly toward contemporary releases\
-   Era-defined programming is visible through release-year metrics

The visualization below maps programming density against contemporary
bias:

![Programming Density vs Freshness](analytics/outputs/density_vs_freshness.png)


Outliers reflect distinct programming strategies rather than random
variation.

------------------------------------------------------------------------

## Example Visualization

`analytics/outputs/density_vs_freshness.png`

Scatter plot showing:

-   X-axis: Unique artists per broadcast hour\
-   Y-axis: % of tracks released within last 5 years\
-   Color intensity: Freshness concentration

Programs cluster into distinct structural archetypes.

------------------------------------------------------------------------

## Tech Stack

-   Python\
-   SQLite\
-   pandas\
-   BeautifulSoup\
-   requests\
-   RapidFuzz\
-   Spotify Web API\
-   matplotlib

------------------------------------------------------------------------

## How to Run

1.  Clone the repository\
2.  Create and activate a virtual environment\
3.  Install dependencies

NOTE: The SQLite database file (radio_plays.db) is generated automatically during ingestion and is not included in the repository.

``` bash
pip install -r requirements.txt
```

4.  Run ingestion:

``` bash
python rs_main.py
```

5.  Run analytics:

``` bash
python analytics/analysis.py
```

6.  Generate visualization:

``` bash
python analytics/visuals.py
```

------------------------------------------------------------------------

## Design Principles

-   Idempotent ingestion\
-   Explicit state tracking\
-   Clear separation between ingestion, normalization, enrichment, and
    analytics\
-   Minimal silent failure\
-   Structured logging and observability

------------------------------------------------------------------------

## Future Extensions

As the dataset grows:

-   Time-series trend modeling\
-   Rotation velocity analysis\
-   Programming archetype clustering\
-   Extended Spotify audio feature analysis

------------------------------------------------------------------------

## Author

Independent quantitative analytics portfolio project.
