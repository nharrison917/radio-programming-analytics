import sqlite3
from scraper.config import DB_PATH
from scraper.normalization_logic import normalize_title_artist  # we’ll create this next


def normalize_new_plays(batch_size=500):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        SELECT COUNT(*) FROM plays
        WHERE norm_title_core IS NULL
           OR norm_artist IS NULL
    """)
    total = cur.fetchone()[0]

    if total == 0:
        print("No new plays to normalize.")
        conn.close()
        return {"normalized": 0}

    print(f"Normalizing {total} plays...")

    processed = 0

    while True:
        cur.execute(f"""
            SELECT id, title, artist
            FROM plays
            WHERE norm_title_core IS NULL
               OR norm_artist IS NULL
            LIMIT {batch_size}
        """)
        rows = cur.fetchall()

        if not rows:
            break

        updates = []

        for pid, title, artist in rows:
            norm = normalize_title_artist(title, artist)

            updates.append((
                norm.get("norm_title_full"),
                norm.get("norm_title_core"),
                norm.get("norm_artist"),
                norm.get("norm_key_full"),
                norm.get("norm_key_core"),
                norm.get("version_note_raw"),
                norm.get("version_note"),
                norm.get("version_type"),
                norm.get("version_year"),
                norm.get("feat_artists_raw"),
                norm.get("feat_artists"),
                norm.get("title_base_raw"),
                pid
            ))

        cur.executemany("""
            UPDATE plays SET
                norm_title_full = ?,
                norm_title_core = ?,
                norm_artist = ?,
                norm_key_full = ?,
                norm_key_core = ?,
                version_note_raw = ?,
                version_note = ?,
                version_type = ?,
                version_year = ?,
                feat_artists_raw = ?,
                feat_artists = ?,
                title_base_raw = ?
            WHERE id = ?
        """, updates)

        conn.commit()
        processed += len(updates)
        print(f"  Processed {processed}/{total}")

    conn.close()
    print("Normalization complete.")

    return {"normalized": total}