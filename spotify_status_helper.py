import sqlite3

DB_PATH = "radio_plays.db"

def update_spotify_status(canonical_id, status_value):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        UPDATE canonical_tracks
        SET spotify_status = ?
        WHERE canonical_id = ?
    """, (status_value, canonical_id))

    conn.commit()
    conn.close()

    print(f"canonical_id {canonical_id} set to {status_value}")

def bulk_update_status(canonical_ids, status_value):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.executemany("""
        UPDATE canonical_tracks
        SET spotify_status = ?
        WHERE canonical_id = ?
    """, [(status_value, cid) for cid in canonical_ids])

    conn.commit()
    conn.close()

    print(f"Updated {len(canonical_ids)} rows to {status_value}")


    ### ------- Use like this
    # python
    # from spotify_status_helper import ____
    # (and then run it like either one of these)
    # update_spotify_status(917, "NON_MUSIC")
    # bulk_update_status([561, 798, 877, 878], "FAILED_PERMANENT")


    