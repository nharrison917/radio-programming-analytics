import sqlite3
from scraper.config import DB_PATH

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

    conn.commit()
    conn.close()

def insert_play(play):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    try:
        cur.execute("""
            INSERT OR IGNORE INTO plays
            (play_ts, station_show, title, artist,
             raw_title, raw_artist, raw_time_text,
             confidence, source_url, scraped_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            play["play_ts"],
            play["station_show"],
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