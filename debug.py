import sqlite3

conn = sqlite3.connect("radio_plays.db")
cur = conn.cursor()

rows = cur.execute("""
SELECT id, play_ts, station_show, title, artist
FROM plays
WHERE title = ?
ORDER BY id DESC
""", ("Seven Nation Army",)).fetchall()

conn.close()

print(f"Found {len(rows)} rows:\n")
for r in rows:
    print(r)