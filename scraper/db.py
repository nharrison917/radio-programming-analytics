import sqlite3
from scraper.config import DB_PATH


def migrate_db():
    """Apply incremental schema changes to an existing database.

    Uses PRAGMA table_info to check for existing columns before adding,
    making each migration step idempotent. Safe to run on a live DB.
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # -- Phase Two: release year accuracy via ISRC + MusicBrainz --
    cur.execute("PRAGMA table_info(canonical_tracks)")
    existing = {row[1] for row in cur.fetchall()}

    new_columns = [
        ("spotify_album_type",    "TEXT"),
        ("spotify_isrc",          "TEXT"),
        ("mb_first_release_year", "INTEGER"),
        ("mb_lookup_status",      "TEXT"),
        ("mb_looked_up_at",       "TEXT"),
    ]
    for col_name, col_type in new_columns:
        if col_name not in existing:
            cur.execute(
                f"ALTER TABLE canonical_tracks ADD COLUMN {col_name} {col_type}"
            )
            print(f"  migrate_db: added column canonical_tracks.{col_name}")

    conn.commit()
    conn.close()


def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS plays (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        play_ts TEXT NOT NULL,
        station_show TEXT,
        title TEXT,
        artist TEXT,
        raw_title TEXT,
        raw_artist TEXT,
        raw_time_text TEXT,
        confidence TEXT,
        source_url TEXT,
        scraped_at TEXT
    )
    """)

    cur.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_play
    ON plays(play_ts, station_show, title, artist)
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS canonical_artists (
        spotify_artist_id   TEXT PRIMARY KEY,
        artist_name         TEXT,
        earliest_release_year INTEGER,
        earliest_release_name TEXT,
        enrichment_status   TEXT DEFAULT 'PENDING',
        last_attempted_at   TEXT,
        attempt_count       INTEGER DEFAULT 0,
        created_at          TEXT
    )
    """)

    conn.commit()
    conn.close()

def insert_play(play):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    cur = conn.cursor()

    try:
        cur.execute("""
            INSERT OR IGNORE INTO plays
            (play_ts,
            station_show,
            is_music_show,
            title,
            artist,
            raw_title,
            raw_artist,
            raw_time_text,
            confidence,
            source_url,
            scraped_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            play["play_ts"],
            play["station_show"],
            play.get("is_music_show"),
            play["title"],
            play["artist"],
            play["raw_title"],
            play["raw_artist"],
            play["raw_time_text"],
            play["confidence"],
            play["source_url"],
            play["scraped_at"],
        ))

        conn.commit()
        return cur.rowcount == 1

    finally:
        conn.close()