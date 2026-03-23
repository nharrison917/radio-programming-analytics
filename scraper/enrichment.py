import sqlite3
import requests
import base64
import time
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from rapidfuzz import fuzz
from scraper.config import DB_PATH
from scraper.normalization_logic import normalize_title, normalize_artist


TITLE_THRESHOLD = 90
ARTIST_THRESHOLD = 85

CHUNK_SIZE = 10
COOLDOWN_SECONDS = 10


def similarity(a, b):
    if not a or not b:
        return 0
    return fuzz.token_set_ratio(a.lower(), b.lower())


def get_spotify_token(client_id, client_secret):
    auth_str = f"{client_id}:{client_secret}"
    b64_auth = base64.b64encode(auth_str.encode()).decode()

    response = requests.post(
        "https://accounts.spotify.com/api/token",
        headers={
            "Authorization": f"Basic {b64_auth}",
            "Content-Type": "application/x-www-form-urlencoded"
        },
        data={"grant_type": "client_credentials"}
    )

    response.raise_for_status()
    return response.json()["access_token"]


def spotify_search_tracks(query, token):
    response = requests.get(
        "https://api.spotify.com/v1/search",
        headers={"Authorization": f"Bearer {token}"},
        params={"q": query, "type": "track", "limit": 5}
    )

    time.sleep(0.5)  # proactive throttle

    if response.status_code == 429:
        retry_after = int(response.headers.get("Retry-After", 1))

        now = datetime.now(ZoneInfo("America/New_York"))
        resume_time = now + timedelta(seconds=retry_after)

        print(f"\nRate limit detected ({retry_after}s). Aborting enrichment.")
        print(
            f"Resume enrichment after "
            f"{resume_time.strftime('%I:%M %p')} "
            f"on {resume_time.strftime('%A, %B %d, %Y')} (ET)\n"
        )

        rate_limit_abort = True
        raise RuntimeError("Rate limit exceeded.")

    if response.status_code != 200:
        return []

    return response.json().get("tracks", {}).get("items", [])


def enrich_all(client_id, client_secret):
    token = get_spotify_token(client_id, client_secret)

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Repair any PENDING records that have been attempted before
    # (can occur if enrichment crashes after incrementing attempt_count
    # but before writing FAILED status)
    cur.execute("""
        UPDATE canonical_tracks
        SET spotify_status = 'FAILED'
        WHERE spotify_status = 'PENDING'
          AND spotify_attempt_count > 0
    """)
    conn.commit()

    cur.execute("""
        SELECT canonical_id, display_title, display_artist,
               norm_title_core, norm_artist,
               spotify_last_attempted_at
        FROM canonical_tracks
        WHERE spotify_album_id IS NULL
            AND (
                spotify_last_attempted_at IS NULL
                OR spotify_last_attempted_at < DATETIME('now', '-2 days')
                )
            AND spotify_status IN ('PENDING', 'FAILED')
    """)

    rows = cur.fetchall()
    total = len(rows)

    attempt_counts = {1: 0, 2: 0, 3: 0, 4: 0}
    failure_count = 0
    new_failure_count = 0
    rate_limit_abort = False
    enriched_this_run = 0

    print(f"Total unenriched canonicals: {total}")

    processed = 0

    try:
        for i in range(0, total, CHUNK_SIZE):

            chunk = rows[i:i+CHUNK_SIZE]
            print(f"\nProcessing chunk {i}–{i+len(chunk)}")

            for row in chunk:
                canonical_id, display_title, display_artist, norm_title, norm_artist, last_attempted_at = row
                is_first_attempt = last_attempted_at is None

                title_score = None
                artist_score = None

                # Update attempt tracking
                cur.execute("""
                    UPDATE canonical_tracks
                    SET spotify_last_attempted_at = DATETIME('now'),
                    spotify_attempt_count = COALESCE(spotify_attempt_count, 0) + 1
                    WHERE canonical_id = ?
                """, (canonical_id,))
                conn.commit()

                # --- Manual override check ---
                cur.execute("""
                    SELECT spotify_id
                    FROM manual_spotify_overrides
                    WHERE canonical_id = ?
                """, (canonical_id,))

                override = cur.fetchone()

                if override:
                    override_spotify_id = override[0]

                    attempt_counts[0] = attempt_counts.get(0, 0) + 1
                    enriched_this_run += 1
                    logging.info(f"Manual override used for canonical_id={canonical_id}")

                    # Fetch track directly
                    track_response = requests.get(
                        f"https://api.spotify.com/v1/tracks/{override_spotify_id}",
                        headers={"Authorization": f"Bearer {token}"}
                    )

                    if track_response.status_code == 200:
                        selected = track_response.json()
                        selected_attempt = 0  # override indicator
                    else:
                        logging.warning(f"Override fetch failed for canonical_id={canonical_id}")
                        continue
                else:
                    selected = None

                if not selected:
                    # run search attempts    
                    attempts = [
                        f'track:"{display_title}" artist:"{display_artist}"',
                        f'track:"{norm_title}" artist:"{display_artist}"',
                        f'track:"{norm_title}"',
                        f'{norm_title} {norm_artist}'
                    ]

                    selected = None
                    selected_attempt = None
                    title_score = None
                    artist_score = None

                    for attempt_number, query in enumerate(attempts, start=1):
                        candidates = spotify_search_tracks(query, token)

                        for track in candidates:
                            spotify_title = track["name"]
                            spotify_artists = [a["name"] for a in track["artists"]]

                            spotify_norm = normalize_title(spotify_title)
                            spotify_core = spotify_norm.get("norm_title_core")

                            spotify_artist_norms = [normalize_artist(a) for a in spotify_artists]

                            t_score = similarity(norm_title, spotify_core)
                            a_score = max(similarity(norm_artist, a) for a in spotify_artist_norms)

                            if t_score >= TITLE_THRESHOLD and a_score >= ARTIST_THRESHOLD:
                                selected = track
                                selected_attempt = attempt_number
                                title_score = t_score
                                artist_score = a_score
                                break

                        if selected:
                            attempt_counts[selected_attempt] += 1
                            enriched_this_run += 1
                            break
            
                    if not selected:
                        failure_count += 1
                        if is_first_attempt:
                            new_failure_count += 1
                            logging.info(f"New failure (first attempt) canonical_id={canonical_id} ({display_artist} - {display_title})")
                        cur.execute("""
                            UPDATE canonical_tracks
                            SET spotify_status = 'FAILED'
                            WHERE canonical_id = ?
                        """, (canonical_id,))
                        conn.commit()

                if selected:
                    album = selected["album"]
                    primary_artist = selected["artists"][0]
                    release_date = album.get("release_date")
                    release_precision = album.get("release_date_precision")

                    release_year = None
                    if release_date:
                        release_year = int(release_date.split("-")[0])

                    current_year = datetime.utcnow().year
                    if release_year is not None and (release_year < 1920 or release_year > current_year + 1):
                        logging.warning(
                            f"Implausible release year {release_year} for "
                            f"canonical_id={canonical_id} ({display_artist} - {display_title}), "
                            f"nulling out release year"
                        )
                        release_year = None

                    cur.execute("""
                        UPDATE canonical_tracks
                        SET spotify_id = ?,
                            spotify_uri = ?,
                            spotify_url = ?,
                            spotify_track_name = ?,
                            spotify_duration_ms = ?,
                            spotify_primary_artist_id = ?,
                            spotify_primary_artist_name = ?,
                            spotify_album_id = ?,
                            spotify_album_name = ?,
                            spotify_album_release_date = ?,
                            spotify_album_release_year = ?,
                            spotify_album_release_date_precision = ?,
                            spotify_match_attempt = ?,
                            spotify_title_score = ?,
                            spotify_artist_score = ?,
                            spotify_enriched_at = ?,
                            spotify_status = 'SUCCESS'
                        WHERE canonical_id = ?
                    """, (
                        selected["id"],
                        selected.get("uri"),
                        selected.get("external_urls", {}).get("spotify"),
                        selected["name"],
                        selected.get("duration_ms"),
                        primary_artist.get("id"),
                        primary_artist.get("name"),
                        album.get("id"),
                        album.get("name"),
                        release_date,
                        release_year,
                        release_precision,
                        selected_attempt,
                        title_score,
                        artist_score,
                        datetime.utcnow().isoformat(),
                        canonical_id
                    ))

                    conn.commit()

            processed += len(chunk)
            print(f"Processed {processed}/{total}")

            if i + CHUNK_SIZE < total:
                print(f"Cooling down {COOLDOWN_SECONDS}s...")
                time.sleep(COOLDOWN_SECONDS)

    except RuntimeError:
        print("Enrichment aborted due to rate limiting.")

        conn.close()
        return {
            "enriched": enriched_this_run,
            "failures": failure_count,
            "new_failures": new_failure_count,
            "attempt_counts": attempt_counts,
            "rate_limit_abort": True
        }

    conn.close()

    return {
        "enriched": enriched_this_run,
        "failures": failure_count,
        "new_failures": new_failure_count,
        "attempt_counts": attempt_counts,
        "rate_limit_abort": rate_limit_abort
    }