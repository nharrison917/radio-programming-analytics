# Phase Three: MusicBrainz Enrichment Extensions

## Scope

Phase Three adds two MusicBrainz enrichment extensions that Phase Two did not address:

1. **Manual MBID overrides (Stages 1-4):** Resolve year data for tracks that Spotify
   permanently failed to match, using hand-curated MB recording IDs.

2. **Artist career-start year via MB (Stage 5):** Replace Spotify-sourced
   `earliest_release_year` in `canonical_artists` with MB release-group data,
   fixing the same remaster/compilation contamination problem that Phase Two
   addressed at the track level.

---

## Part A: Manual MBID Overrides for Spotify-FAILED Tracks

### Problem statement

33 tracks in `canonical_tracks` remain `spotify_status = "FAILED"` after repeated
enrichment attempts. These tracks have no Spotify ID and therefore no ISRC, which
means the existing MusicBrainz ISRC-lookup pipeline (Phase Two) cannot help them.

Some of these tracks exist in MusicBrainz under their MBID (recording ID) but are
not on Spotify at all, or exist in a form that Spotify's search consistently misses.
Others are station-exclusive recordings that exist in no external database.

## Failure taxonomy

The 33 FAILED records cluster into three categories:

| Category | Examples | Action |
|---|---|---|
| Station-exclusive recordings | `jason mraz, Rocket Man (Peak Performance)`, `eggy, Helplessly Hoping (Beach Session)` | Mark `NO_MATCH` -- these will never resolve |
| Covers / variant recordings findable in MB | `lumineers, The Weight`, `foo fighters, Baker Street` | Manual MBID lookup |
| Mangled artist normalization | `peter gabriel and hot`, `human sexual respons` | Fix normalization, retry Spotify |

Phase Three addresses the middle category. The station-exclusive category should be
handled first (manual `NO_MATCH` flags) to stop retry noise. The normalization category
is a separate bug in `normalization_logic.py` or the canonical's `display_artist`.

---

## Approach

Mirror the existing `manual_spotify_overrides` pattern: a hand-curated table of IDs
that a script resolves into metadata. The difference is we store a MB recording MBID
instead of a Spotify track ID, and the only output is `mb_isrc_year`.

These tracks remain `spotify_status = "FAILED"` -- we are not resolving them to
Spotify. We are giving them a year so they participate correctly in year-dependent
analytics. The `best_year` CASE expression already in the analytics layer will pick
up `mb_isrc_year` automatically once populated.

---

## Stage 1 -- Schema

**File:** `scraper/db.py`

Add one new table:

```sql
CREATE TABLE IF NOT EXISTS manual_mb_overrides (
    canonical_id    INTEGER PRIMARY KEY,
    mb_recording_id TEXT NOT NULL,
    note            TEXT,
    added_at        TEXT DEFAULT (DATETIME('now'))
);
```

`canonical_id` references `canonical_tracks.canonical_id`.
`mb_recording_id` is the MB recording MBID (UUID format, e.g. `a1b2c3d4-...`).
`note` is optional free text (e.g. "Lumineers cover set, KEXP session").

No foreign key constraint -- SQLite FK enforcement is off by default and we do not
want a failed insert to cascade.

**Acceptance criteria:**
- Table created by `init_db()` on fresh DB
- `ALTER TABLE`-style migration safe to run on existing DB (CREATE IF NOT EXISTS)

---

## Stage 2 -- Lookup script

**New file:** `scraper/mb_manual.py`

Reads all rows from `manual_mb_overrides` where `canonical_tracks.mb_isrc_year`
is NULL or `mb_lookup_status` is not already `SUCCESS`.

For each row:

1. Call `GET https://musicbrainz.org/ws/2/recording/{mbid}?inc=releases&fmt=json`
2. Extract `first-release-date` from the recording object (YYYY, YYYY-MM, or YYYY-MM-DD)
3. Validate year against 1920--current_year+1 bounds (same rule as Phase Two)
4. Write `mb_isrc_year` and `mb_lookup_status = "SUCCESS"` to `canonical_tracks`
   on success, or `mb_lookup_status = "FAILED"` if the MBID returns no usable date

Rate limit: 1 request/second (same MB requirement as Phase Two). Sleep after each call.

User-Agent header required -- reuse `MB_USER_AGENT` constant from `mb_enrichment.py`
or promote it to `config.py`.

### Key difference from ISRC lookup

ISRC lookup returns a list of recordings and takes the earliest year across all of them.
A direct MBID lookup returns a single recording -- the year comes from
`first-release-date` on the recording itself, not from a cross-recording min.

If `first-release-date` is absent on the recording object, fall back to the earliest
release date in the `releases` list (the `inc=releases` include brings these in).
Take the min valid year across all releases in that list.

### Output

Print a summary after each run:
- MBIDs attempted
- SUCCESS / FAILED counts
- For each SUCCESS: artist, title, year written

**Acceptance criteria:**
- `lumineers, The Weight` writes year 1968 (The Band original)
- Rate limiting respected
- Re-running is idempotent (skips already-SUCCESS records)
- Lookup failure writes FAILED status, does not crash the run

---

## Stage 3 -- Entry point

**File:** `rs_main.py`

Add a new mode `mb-manual`:

```bash
python rs_main.py mb-manual
```

Calls `scraper/mb_manual.py::run_mb_manual()`.

This is intentionally separate from `mb-enrich` (which is ISRC-based and runs on
Spotify-SUCCESS records). The two pipelines target different populations.

No scheduling needed -- this is a manual, as-needed operation.

---

## Stage 4 -- Triage: marking station-exclusive recordings NO_MATCH

Before or alongside the above, mark the station-exclusive recordings with
`spotify_status = "NO_MATCH"` to stop the weekly enrichment run from retrying them.

These are recordings with title suffixes like:
- `(Peak Performance)` -- in-studio session for The Peak
- `(Beach Session)`, `(Summer Session)` -- outdoor sessions
- `(Concert For Nyc)` -- one-off event recording
- `(Acoustic Live)` -- live performance, no commercial release

These should not be automated -- each is a judgment call. Do this via direct SQL
or a small scratch script, and document the changes in a comment column or the
existing `note` mechanism if one is added.

After marking, rerun `python rs_main.py analyze` to confirm these tracks no longer
appear in the failures CSV.

---

---

## Part B: Artist Career-Start Year via MusicBrainz

### Problem statement

`canonical_artists.earliest_release_year` is populated by `artist_enrichment.py`,
which paginates through an artist's Spotify discography and takes the minimum album
year. It has the same remaster/compilation contamination as track years: a 2009
remaster appearing early in Spotify's sort can corrupt the "earliest release" reading
for an artist, making them appear to have started their career much later than they did.

MusicBrainz release-groups carry original `first-release-date` values and are
authoritative for career-start year in a way Spotify's album list is not.

### Stage 5 -- Artist MB enrichment

**New file:** `scraper/mb_artist_enrichment.py`
**Entry point:** `python rs_main.py mb-artists`

#### Schema additions

Add to `canonical_artists` via `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`:

| Column | Type | Purpose |
|---|---|---|
| `mb_artist_id` | TEXT | MB artist MBID (UUID) |
| `mb_earliest_release_year` | INTEGER | Earliest release-group year from MB |
| `mb_artist_lookup_status` | TEXT | PENDING / SUCCESS / FAILED / NO_MBID |
| `mb_artist_looked_up_at` | TEXT | Timestamp of last attempt |

Migration must be idempotent (safe to run on existing DB).

#### Lookup process (2-3 calls per artist)

**Step 1 -- Resolve Spotify artist ID to MB artist MBID**

```
GET /ws/2/url?resource=https://open.spotify.com/artist/{spotify_id}&inc=artist-rels&fmt=json
```

Returns URL relationships including the linked MB artist entity. Extract the
`artist` relation's `id` field (the MB artist MBID).

If no relationship exists: set `mb_artist_lookup_status = "NO_MBID"`, skip.
This is expected for artists with low MB coverage (local bands, newer artists).

**Step 2 -- Browse release-groups for the MB artist**

```
GET /ws/2/release-group?artist={mbid}&limit=100&fmt=json
```

Each release-group has a `first-release-date` (YYYY, YYYY-MM, or YYYY-MM-DD).
If artist has more than 100 release-groups, page using `offset` until
`release-group-count` is exhausted. Most artists fit in 1-2 pages.

Take the minimum valid year across all release-groups (same 1920--current_year+1
bounds as elsewhere).

**Step 3 -- Write result**

On success: write `mb_artist_id`, `mb_earliest_release_year`,
`mb_artist_lookup_status = "SUCCESS"`, `mb_artist_looked_up_at = now`.

On failure (no valid year found): `mb_artist_lookup_status = "FAILED"`.

#### Rate limiting

1 request/second (MB requirement). Each artist requires 2 calls minimum
(URL lookup + release-group browse), more if pagination is needed.

Estimated volume: ~500 canonical artists x 2-3 calls = 1,000-1,500 calls.
Estimated runtime: ~25-30 minutes for a full run.

Idempotent: skip artists already marked SUCCESS. Retry FAILED artists after
7 days (same pattern as other MB lookups).

#### best_year rule for artists

Same logic as tracks: only accept MB year if it is strictly earlier than Spotify's.

```python
mb_earliest_release_year
    if mb_earliest_release_year is not None
    and mb_earliest_release_year < spotify_earliest_release_year
    else spotify_earliest_release_year
```

This handles cases where MB returns a remaster or later release as the
"earliest" due to incomplete catalog coverage.

#### Sampling step before full run

Before running on all artists, sample 20-30 already-enriched `canonical_artists`
and compare Spotify's `earliest_release_year` against the MB release-group result.
If the difference is negligible (< 5 artists showing meaningful corrections), the
work may not be worth the operational complexity. Document the finding and defer.

Suggested sample query:
```sql
SELECT artist_name, earliest_release_year
FROM canonical_artists
WHERE enrichment_status = 'SUCCESS'
  AND earliest_release_year IS NOT NULL
ORDER BY earliest_release_year ASC
LIMIT 30;
```

Run these through the MB URL lookup manually (or a small scratch script) and compare.

#### Output

Print after each run:
- Artists attempted / SUCCESS / FAILED / NO_MBID
- Artists where MB year differs from Spotify year (largest corrections first)

#### Acceptance criteria

- David Bowie `mb_earliest_release_year` = 1964 or earlier (first Decca single)
- Spotify-sourced `earliest_release_year` for at least one artist is measurably
  corrected (quantified in the sampling step)
- Pagination handles artists with > 100 release-groups without missing releases
- NO_MBID artists are logged but do not count as failures
- Run is idempotent

---

## Limitations

1. **Year semantics for covers:** `mb_isrc_year` for a Lumineers cover of
   "The Weight" may resolve to 1968 (The Band original) if MB has no entry for the
   Lumineers recording itself. Whether that year is the right attribution depends on
   analysis context -- it is correct for "when was this song written" but wrong for
   "when did The Lumineers perform it." Document this in the analytics layer.

2. **No Spotify metadata:** These tracks will always lack `spotify_album_type`,
   `spotify_primary_artist_id`, and other Spotify fields. Any analytics that
   require Spotify metadata will exclude them. This is acceptable given the volume
   (33 of ~2,559 total).

3. **Manual maintenance:** The `manual_mb_overrides` table requires a human to look
   up and enter MBIDs. This is intentional -- automated text search has poor
   precision for this population. As new FAILED records accumulate, they require
   periodic triage.

4. **MB artist coverage gaps:** MB's URL relationship linking is strongest for
   established artists. Newer or regional artists may have no Spotify-to-MB link,
   producing NO_MBID outcomes. These are expected and not errors.

5. **Release-group completeness:** MB release-group coverage is community-maintained.
   An artist's first single may be absent, causing MB to report a later year than
   Spotify. The "only accept if earlier" rule handles this conservatively -- Spotify's
   value is kept when MB can't improve on it.

---

## Implementation order

### Part A (manual MBID overrides)
1. Mark station-exclusive recordings as `NO_MATCH` (no code needed -- SQL only)
2. Stage 1 (schema) -- add `manual_mb_overrides` table to `db.py`
3. Stage 2 (lookup script) -- `scraper/mb_manual.py`
4. Stage 3 (entry point) -- wire `mb-manual` into `rs_main.py`
5. Populate `manual_mb_overrides` with MBIDs found manually
6. Run `python rs_main.py mb-manual` and verify year corrections

### Part B (artist career-start year)
7. Run sampling comparison (20-30 artists) to validate effort is worth it
8. Stage 5 (schema) -- add four columns to `canonical_artists`
9. Stage 5 (script) -- `scraper/mb_artist_enrichment.py`
10. Wire `mb-artists` into `rs_main.py`
11. Run on full `canonical_artists` population; review corrections
12. Update any analytics that use `earliest_release_year` to use MB-corrected value

## Verification checklist

### Part A
- [ ] Station-exclusive recordings no longer appear in `enrichment_failures.csv`
- [ ] `manual_mb_overrides` table exists in DB
- [ ] `python rs_main.py mb-manual` runs cleanly with 0 rows in the override table
- [ ] After adding one MBID manually, `mb-manual` writes the correct year
- [ ] `best_year` in analytics picks up the new year (confirm via era continuity or box plot)
- [ ] Re-running `mb-manual` is idempotent

### Part B
- [ ] Sampling step completed and correction rate documented before full run
- [ ] Four new columns present in `canonical_artists`
- [ ] `python rs_main.py mb-artists` runs and prints a summary
- [ ] At least one artist shows a corrected `mb_earliest_release_year` earlier than Spotify's
- [ ] NO_MBID artists are counted separately, not as failures
- [ ] Run is idempotent (re-running skips SUCCESS artists)

---

## Manual Override CLI

**Implemented alongside Phase Three triage.** Two commands for hand-correcting individual
records without touching the enrichment pipeline directly.

### Schema additions (applied automatically on first use via `migrate_db`)

| Column | Table | Type | Purpose |
|---|---|---|---|
| `manual_duration_ms` | `canonical_tracks` | INTEGER | Hand-entered duration for tracks not on Spotify |
| `manual_release_date` | `canonical_tracks` | TEXT | Full date string (YYYY or YYYY-MM-DD) for audit trail |

`manual_year_override` (existing) is the integer that `best_year` uses.
`manual_release_date` stores whatever precision you have (YYYY-MM-DD if known, YYYY if not)
and does not affect any query logic -- it exists for provenance only.

`manual_duration_ms` is separate from `spotify_duration_ms` so that a future Spotify
match (if one ever appears) does not silently overwrite a hand-entered value.

---

### `add-override` -- supply a Spotify ID for a FAILED track

Use when the track exists on Spotify but our search failed (normalization error,
title mismatch, truncated artist name). Inserts into `manual_spotify_overrides`.
The next `python rs_main.py weekly` run enriches the track fully (year, duration,
ISRC, artist ID, everything).

```bash
python rs_main.py add-override --id <canonical_id> --spotify-id <spotify_track_id>
```

**Example:**

```
python rs_main.py add-override --id 2779 --spotify-id 4iV5W9uYEdYUVa79Axb7Rh

canonical 2779 | Csny - Woodstock
  spotify_status   : FAILED
  current override : (none)

Setting: spotify_id=4iV5W9uYEdYUVa79Axb7Rh
Proceed? [y/N]: y
Override saved. Run 'python rs_main.py weekly' to enrich.
```

**When to use:** normalization failures (truncated artist names, `W/` notation,
missing letters), title mismatches (Cloud 9 vs Cloud Nine), any FAILED track you
can locate manually on Spotify.

**When not to use:** tracks that are genuinely not on Spotify. Use `set-meta` instead.

---

### `set-meta` -- manually set year and/or duration

Use when the track is not on Spotify but you have year/duration from another source
(MusicBrainz, Discogs, AllMusic, etc.). Writes directly to `canonical_tracks`.

```bash
python rs_main.py set-meta --id <canonical_id> [--year YYYY|YYYY-MM-DD] [--duration M:SS]
```

At least one of `--year` or `--duration` is required. Both can be supplied together.

**Examples:**

```bash
# Year and duration together
python rs_main.py set-meta --id 1677 --year 2016-03-15 --duration 3:33

# Year only (date precision if known, year-only if not)
python rs_main.py set-meta --id 1677 --year 2016

# Duration only (e.g. year already set, adding duration later)
python rs_main.py set-meta --id 1677 --duration 3:33
```

**Sample output:**

```
canonical 1677 | Blonde Diamond - Feel Alright
  spotify_status             : FAILED
  spotify_album_release_year : (none)
  manual_year_override       : (none)
  manual_release_date        : (none)
  manual_duration_ms         : (none)

Setting: year=2016, release_date='2016-03-15', duration=3:33 (213000 ms)
Proceed? [y/N]: y
Saved.
```

**Duration format:** `M:SS` or `MM:SS` (e.g. `3:33`, `12:04`). Converted to
milliseconds internally. Seconds must be 00-59.

**Year format:** `YYYY` or `YYYY-MM-DD`. The integer year is written to
`manual_year_override` (used by `best_year`). The full string is written to
`manual_release_date` (audit trail only).

**Note on chaining with `add-override`:** If you add a Spotify override and also have
the duration handy, you can run `set-meta --duration` on the same canonical immediately.
The `weekly` run will not overwrite `manual_duration_ms`.

---

### Triage reference: which command to use

| Situation | Command |
|---|---|
| Track on Spotify, search failed (normalization, title mismatch) | `add-override` |
| Track not on Spotify, found on MB/Discogs | `set-meta` |
| Track not on Spotify, year known but not duration | `set-meta --year` |
| Track is a station session / cover with no release -- will never resolve | SQL: set `spotify_status = 'NO_MATCH'` directly |
