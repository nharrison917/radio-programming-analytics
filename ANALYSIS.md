# ANALYSIS.md - Radio Programming Analytics: Findings and Methods

Running record of analytical work, methods, and findings for the radio programming
pipeline. Updated as new analyses are built.

------------------------------------------------------------------------

## Show Clustering

**Script:** `analytics/show_clustering.py`
**Run:** `python rs_main.py cluster`
**Outputs:** `analytics/outputs/clustering/cluster_*.html`, `analytics/outputs/clustering/show_clustering_features.csv`

### Question

Are the 11 station shows meaningfully differentiated in how they program music,
and if so, along what dimensions?

### Method

Four clustering passes, each using Ward linkage hierarchical clustering:

**Pass 1 -- Scalar features** (intrinsic show characteristics)

| Feature | What it captures |
|---|---|
| `artist_entropy` | Shannon entropy of artist play distribution -- higher = more even spread |
| `unique_artists_per_hour` | Distinct artists per broadcast hour -- rotation speed |
| `avg_best_year` | Mean release year using best available source (MusicBrainz > Spotify) |
| `freshness_pct` | Share of plays from tracks released in the last 5 years |
| `exclusive_artist_pct` | Share of a show's artists that appear on no other show |
| `era_continuity_mean_gap` | Average year-gap between consecutive plays -- era mixing vs. consistency |

All features z-scored before distance calculation. Euclidean distance, Ward linkage.

**Pass 2 -- Repertoire similarity** (what is actually being programmed)

Binary cosine similarity over a rolling 60-day window. Each show is represented
as a binary vector: top-10 most-played artists + top-20 most-played tracks = 30
indicator dimensions per show. Union vocabulary across all shows (153 dimensions
total). Cosine distance fed directly to Ward linkage.

The 60-day window is intentional -- repertoire is a temporal signal. Re-running
this analysis after several weeks will produce different results as playlists evolve.

**Pass 3 -- Combined, unweighted** (scalar features + MDS embedding of repertoire)

The repertoire similarity matrix is not directly combinable with scalar features.
It is first reduced to 2 dimensions via MDS (metric=precomputed, random_state=42),
then concatenated with the 6 scalar features (8 total). All features z-scored.
This gives the repertoire signal a 2:6 vote share vs. the scalar features.

**Pass 4 -- Combined, equal-weight** (same as Pass 3 but MDS dimensions scaled x3)

The MDS coordinates are multiplied by 3 before StandardScaler rescaling, giving
the repertoire family approximately equal influence to the scalar family (6v each).
This tests whether the Pass 3 structure is an artifact of scalar overweighting.

### Findings

**Consistent three-cluster structure across all four passes:**

- **Main rotation core** -- Coach, Peak Music, Chris Herrmann, Jimmy Fink, Pam Landry.
  Tight cluster on both scalars and repertoire. Jimmy Fink and Peak Music are the closest
  pair in the dataset (cosine similarity 0.77 on repertoire). These shows share
  the same artists (U2, Black Keys, Noah Kahan, Tedeschi Trucks, Bruce Springsteen)
  and draw from the same track pool.

- **Oldies tier** -- 10@10 and 10@10 Weekend Replay (cosine similarity 0.73, expected
  as the Replay is a rebroadcast). Average release year 1979. Adjacent to the main
  core in scalar space but clearly separated by era and repertoire. Andy Bale sits
  between this tier and the core in the combined passes.

- **Specialty outliers** -- Three shows with near-zero repertoire overlap with
  all other shows:
  - *90's at Night*: cosine similarity 0.00 with every other show. Programs a
    completely distinct catalog (Nirvana, Pearl Jam, Red Hot Chili Peppers, etc.).
    Tightest era sequencing among the specialty outliers (mean_gap = 4.1 yrs).
  - *Sunday Mornings Over Easy*: near-zero similarity (0.00-0.10). Folk/acoustic/
    Americana format (Grateful Dead, Norah Jones, Bob Dylan, Iron and Wine).
    High exclusive_artist_pct despite not being a genre-locked format.
  - *This Just In with Meg White*: low but non-zero similarity (0.10-0.20).
    Contemporary/indie lean (Noah Kahan, Sheepdogs, Metric, Bleachers). Extreme
    outlier on freshness (100%) and avg_best_year (2025.8). Also the tightest era
    sequencing in the dataset (mean_gap = 0.34), alongside 10@10 (0.60) -- both
    are density-segmented shows with rigid single-era formats.

**Cluster robustness:**

Passes 3 and 4 (combined unweighted vs. equal-weight) produce virtually identical
dendrograms. Tripling the repertoire weight does not shift any cluster assignments.
This means the scalar and repertoire signals are not in tension -- shows that program
similarly on operational metrics also share similar playlists. The clustering structure
is stable, not an artifact of feature weighting.

**Observed differentiation within the main rotation core:**

Andy Bale and Pam Landry appear closer on scalar features alone (similar era mix,
freshness, rotation depth) but diverge on repertoire. Chris Herrmann and Coach show
the same pattern. Both pairs are pulled back together in the combined passes, confirming
that the repertoire divergence is real but not large enough to overcome the strong
scalar similarity. These pairs have the same programming philosophy but slightly
different personal taste -- they would feel similar to a casual listener.

**Scalar concentration:**

Top-20 artist coverage per show ranges from 19% (Peak Music) to 46% (90's at Night).
The main rotation shows are low-concentration, long-tail programmers. A top-N overlap
approach to measuring style similarity was therefore discarded in favour of binary
indicator vectors and concentration scalars.

**Key insight:**

The scalar features measure *how* a show is programmed; the repertoire features measure
*what* is programmed. The scalar space is primarily capturing format type (current
rotation vs. oldies vs. specialty). The main rotation shows converge on similar scalar
values because they operate within the same format constraints. The repertoire dimension
is where individual curation choices live -- but even there, the main rotation shows
are drawing from a shared pool with limited individual differentiation. The clearest
differentiations in the dataset are at the format boundary, not the host boundary.

------------------------------------------------------------------------

## Notes

### Show-to-hour attribution

The scraper attributes plays to shows by hour, matching the station website's structure.
Several shows have a structural mismatch between the scraped hour boundary and the actual
show boundary.

**Known cases:**

- **"10 @ 10" / "10 @ 10 Weekend Replay"** -- bleed tracks from regular rotation surround
  the single-era themed segment. Handled via density-based segmentation.
- **"This Just In with Meg White"** -- intentional 1-2 track throwback tail at :50-:59.
  Handled via segmentation.
- **"90's at Night"** -- a handful of non-90s plays at the very start of the 20:00 hour,
  likely bleed from whatever aired before. Most apparent anomalies are remaster/compilation
  year artifacts resolved by MB enrichment.

**Open question:** correct data to recover programmatic intent (reclassify bleed tracks to
the adjacent show), or treat data as an honest record of what the website reported at scrape
time? Both positions are defensible; "as recorded" is reproducible and makes no assumptions
about intent. No decision made.

### "90's at Night" -- segmentation deferred

Examined 2026-04-10. 193 enriched plays across 8 airing dates (16 hour-blocks):

- 96.4% of plays fall within 1988-2005 (the expected 90s range)
- 7/193 OOB tracks appear at scattered positions within the hour -- not front-loaded
  as originally hypothesised
- OOB tracks are post-2005 modern tracks, not a systematic bleed pattern

Segmentation would produce nearly identical metrics. Not added to `SEGMENT_SHOWS`.

**If revisited:** `_modal_era` infers era from density, which is not ideal for a
fixed-format decade show. A fixed center (1995, band ~7yr) would be more principled
than density inference. A temporal filter (exclude first N minutes of the 20:00 hour)
is also worth testing if the front-loading hypothesis strengthens with more data.
