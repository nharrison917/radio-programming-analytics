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

---

## `set-artist-meta` -- correct a wrong artist MBID

Use when `mb_artist_large_delta.csv` or Pass A logs reveal that the wrong MB entity
was resolved for an artist. Updates `mb_artist_id`, clears the existing Pass B result,
and immediately re-runs Pass B so the corrected year is visible right away.

```bash
python rs_main.py set-artist-meta --artist-name "Dada" --mb-id "0c90c74b-a53b-4740-a508-c0669b2cb74d"
```

`--artist-name` must match `canonical_artists.artist_name` exactly (case-insensitive).
The MBID must be a valid MusicBrainz artist UUID.

**Sample output:**

```
Artist  : Dada
Old MBID: bad-mbid-here  (year was 1982)
New MBID: 0c90c74b-a53b-4740-a508-c0669b2cb74d

Running Pass B for this artist...
mb_earliest_release_year = 1990  (status=SUCCESS)
```

If Pass B returns no year, `status=FAILED` -- double-check the MBID is correct.

---

## Artist NO_MATCH -- close an artist from future enrichment

Use when an artist is genuinely absent from MusicBrainz (no relevant entity exists),
or when the only MB entity has no usable release data. Sets `mb_artist_status = 'NO_MATCH'`
so Pass A never retries this artist.

```bash
python -c "
import sqlite3
conn = sqlite3.connect('radio_plays.db')
conn.execute(\"\"\"
    UPDATE canonical_artists
    SET mb_artist_status = 'NO_MATCH'
    WHERE LOWER(artist_name) = LOWER('Artist Name Here')
\"\"\")
conn.commit()
conn.close()
print('Done')
"
```

Artists with `NO_MATCH` status are excluded from `mb_artist_missing.csv` and will not
appear in enrichment runs. The status is permanent unless manually cleared.

---

## Finding the right MBID

Search at [musicbrainz.org](https://musicbrainz.org). Before using a result:

1. **Check the entity type and begin date** -- confirm it matches the artist's known
   career start. A Person and a Group of the same name are different entities.
2. **Watch for disambiguation** -- common names (Everything, Dada, Rockets) often have
   multiple MB entries. The top result by score is not always the right one.
3. **Solo artist vs. band credit** -- if the play lists "Rob Thomas" (not "Matchbox Twenty"),
   use Rob Thomas's solo MBID. The metric measures career age *as credited on that play*.
4. **Verify by browsing release-groups** -- look at the entity's releases page to confirm
   the earliest album/single makes sense for the artist you know.

The MBID is the UUID in the entity's URL:
`musicbrainz.org/artist/`**`9727e632-c228-47f8-a7ae-398df94f00c7`**

---

## Triaging the MB artist quality reports

Two CSVs in `analytics/outputs/quality_checks/` track open artist enrichment issues.
Both are regenerated at the end of every `python rs_main.py mb-artist-enrich` run.

### `mb_artist_missing.csv`

Artists where Pass A could not resolve a MBID. Columns:
- `mb_artist_status` -- `NULL` (both ISRC and name search failed) or `FAILED` (Pass B ran but found no releases)
- `has_isrc` -- if 0, only name search was available to Pass A
- `play_count` -- use to prioritise which missing artists matter most

**Action:** look up the artist on MusicBrainz and run `set-artist-meta`, or run the
NO_MATCH SQL if they're genuinely absent.

### `mb_artist_large_delta.csv`

Artists where `|mb_earliest_release_year - spotify_earliest_year| > 5`. Sorted by
`abs_delta` descending.

- `mb_later_than_spotify = 1` -- MB found a *later* start date than Spotify. This is the
  suspicious direction: it usually means a wrong MBID, a future-dated MB release, or a
  Live/misattributed release slipping through. **Review these first.**
- `mb_later_than_spotify = 0` -- MB found an *earlier* start date than Spotify. This is
  usually correct: Spotify only surfaces what's available on the platform, while MB
  covers the full career including pre-streaming-era releases. Most of these are fine.

**Action for `mb_later_than_spotify = 1` rows:** verify the MBID on musicbrainz.org.
If wrong entity: `set-artist-meta`. If the MBID is correct but MB has bad date data
(e.g. a future pre-registration or a Live album with a typo): NO_MATCH SQL.

---

## Triage reference: which command to use

### Track-level

| Situation | Command |
|---|---|
| Track on Spotify, search failed (normalization, title mismatch) | `add-override` |
| Track not on Spotify, found on MB/Discogs | `set-meta` |
| Track not on Spotify, year known but not duration | `set-meta --year` |
| Track is a station session / cover with no release -- will never resolve | SQL: set `spotify_status = 'NO_MATCH'` directly |

### Artist-level

| Situation | Command |
|---|---|
| Wrong MBID resolved by Pass A (wrong entity, wrong primary credit) | `set-artist-meta` |
| Artist not in MusicBrainz, or no usable release data | NO_MATCH SQL |
| Large delta, MB later than Spotify (`mb_later_than_spotify = 1`) | Verify MBID; then `set-artist-meta` or NO_MATCH SQL |
| Large delta, MB earlier than Spotify (`mb_later_than_spotify = 0`) | Usually correct -- no action needed |
