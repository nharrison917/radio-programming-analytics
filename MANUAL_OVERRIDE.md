# Manual Override CLI

Two commands for hand-correcting individual records without touching the enrichment pipeline.
Implemented in `scraper/overrides.py`; entry points via `rs_main.py`.

```bash
python rs_main.py add-override --id <canonical_id> --spotify-id <spotify_track_id>
python rs_main.py set-meta --id <canonical_id> [--year YYYY|YYYY-MM-DD] [--duration M:SS]
```

## Schema additions

Applied automatically on first use via `migrate_db`.

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

## `add-override` -- supply a Spotify ID for a FAILED track

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

## `set-meta` -- manually set year and/or duration

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

## Triage reference: which command to use

| Situation | Command |
|---|---|
| Track on Spotify, search failed (normalization, title mismatch) | `add-override` |
| Track not on Spotify, found on MB/Discogs | `set-meta` |
| Track not on Spotify, year known but not duration | `set-meta --year` |
| Track is a station session / cover with no release -- will never resolve | SQL: set `spotify_status = 'NO_MATCH'` directly |
