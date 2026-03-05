import sqlite3
from scraper.config import DB_PATH


def seed_new_canonicals():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO canonical_tracks (
            norm_key_core,
            norm_artist,
            norm_title_core,
            display_artist,
            display_title,
            example_play_id,
            play_count,
            first_play_ts,
            last_play_ts,
            created_at,
            created_by
        )
        SELECT
            p.norm_key_core,
            p.norm_artist,
            p.norm_title_core,
            MIN(p.artist),
            MIN(p.title),
            MIN(p.id),
            COUNT(*),
            MIN(p.play_ts),
            MAX(p.play_ts),
            DATETIME('now'),
            'auto_seed'
        FROM plays p
        LEFT JOIN canonical_tracks c
            ON p.norm_key_core = c.norm_key_core
        WHERE c.norm_key_core IS NULL
          AND p.norm_key_core IS NOT NULL
        GROUP BY p.norm_key_core
    """)

    inserted = cur.rowcount
    conn.commit()
    conn.close()

    print(f"Seeded {inserted} new canonicals")

    return {"canonicals_seeded": inserted}


def map_new_plays():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO plays_to_canonical (
            play_id,
            canonical_id,
            match_method,
            match_score,
            is_approved,
            approved_at,
            created_at,
            created_by
        )
        SELECT
            p.id,
            c.canonical_id,
            'norm_key_core',
            1.0,
            1,
            DATETIME('now'),
            DATETIME('now'),
            'auto_seed'
        FROM plays p
        JOIN canonical_tracks c
            ON p.norm_key_core = c.norm_key_core
        LEFT JOIN plays_to_canonical m
            ON p.id = m.play_id
        WHERE m.play_id IS NULL
    """)

    inserted = cur.rowcount
    conn.commit()
    conn.close()

    print(f"Mapped {inserted} new plays")

    return {"plays_mapped": inserted}