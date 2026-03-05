from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from urllib.parse import urlencode

from scraper.config import (
    BASE_PLAYED_URL, CRAWL_DELAY,
    MIN_PLAYS_PER_HOUR,
    MAX_PLAYS_PER_HOUR,
    FLAG_NULL_STATION_SHOW,
    FLAG_SUSPICIOUS_TITLE,
)
from scraper.db import init_db, insert_play
from scraper.fetch import fetch_url
from scraper.parsing import parse_played_page
from scraper.utils import setup_logging, create_backup
import sqlite3
import time
import logging


def build_play_url(target_date, hour):
    params = {
        "date": target_date.strftime("%Y-%m-%d"),
        "hour": str(hour)
    }
    return BASE_PLAYED_URL + "?" + urlencode(params)


def get_last_play_ts():
    conn = sqlite3.connect("radio_plays.db")
    cur = conn.cursor()
    cur.execute("SELECT MAX(play_ts) FROM plays;")
    row = cur.fetchone()
    conn.close()

    if row and row[0]:
        return datetime.fromisoformat(row[0])
    return None


def run_scrape():
    setup_logging("scrape")
    logging.info("Starting scrape job")
    create_backup()

    init_db()

    ny_tz = ZoneInfo("America/New_York")
    now_ny = datetime.now(ny_tz)

    today = now_ny.date()
    last_completed_hour = now_ny.hour - 1

    if last_completed_hour < 0:
        logging.info("No completed hours yet today.")
        return

    last_play_dt = get_last_play_ts()

    if last_play_dt:
        last_hour = last_play_dt.replace(minute=0, second=0, microsecond=0)
        start_dt = last_hour - timedelta(hours=1)
    else:
        start_dt = datetime(today.year, today.month, today.day, 0, 0)

    end_dt = datetime(today.year, today.month, today.day, last_completed_hour, 0)

    logging.info(f"Scrape window start={start_dt} end={end_dt}")

    total_seen = 0
    total_inserted = 0
    hourly_counts = {}
    anomalies = []

    pages_attempted = 0
    pages_fetched = 0
    first_hour = None
    last_hour = None

    current_dt = start_dt

    # ---------------- SCRAPE LOOP ----------------
    while current_dt <= end_dt:
        pages_attempted += 1

        if first_hour is None:
            first_hour = current_dt.strftime("%Y-%m-%dT%H")
        last_hour = current_dt.strftime("%Y-%m-%dT%H")

        url = build_play_url(current_dt.date(), current_dt.hour)
        logging.info(f"Fetching {url}")

        try:
            html = fetch_url(url)
            pages_fetched += 1
        except Exception as e:
            logging.warning(f"Failed to fetch {url}: {e}")
            current_dt += timedelta(hours=1)
            continue

        plays = parse_played_page(html, url)

        hour_key = current_dt.strftime("%Y-%m-%dT%H")
        hourly_counts[hour_key] = len(plays)

        total_seen += len(plays)

        inserted = 0
        for p in plays:
            if insert_play(p):
                inserted += 1

        total_inserted += inserted

        logging.info(f"Hour={hour_key} parsed={len(plays)} inserted={inserted}")

        time.sleep(CRAWL_DELAY)
        current_dt += timedelta(hours=1)

    # ---------------- ANOMALY CHECKS ----------------

    for hour, count in hourly_counts.items():
        if count < MIN_PLAYS_PER_HOUR:
            msg = f"hourly_low_play_count hour={hour} plays={count}"
            logging.warning(msg)
            anomalies.append(msg)

        if count > MAX_PLAYS_PER_HOUR:
            msg = f"hourly_high_play_count hour={hour} plays={count}"
            logging.warning(msg)
            anomalies.append(msg)

    if FLAG_NULL_STATION_SHOW:
        conn = sqlite3.connect("radio_plays.db")
        cur = conn.cursor()

        cur.execute("""
            SELECT COUNT(*) FROM plays
            WHERE station_show IS NULL
              AND play_ts BETWEEN ? AND ?
        """, (start_dt.isoformat(), end_dt.isoformat()))

        null_count = cur.fetchone()[0]
        conn.close()

        if null_count > 0:
            msg = f"station_show_null count={null_count}"
            logging.warning(msg)
            anomalies.append(msg)

    if FLAG_SUSPICIOUS_TITLE:
        conn = sqlite3.connect("radio_plays.db")
        cur = conn.cursor()

        cur.execute("""
            SELECT id, play_ts, station_show, title, source_url
            FROM plays
            WHERE play_ts BETWEEN ? AND ?
        """, (start_dt.isoformat(), end_dt.isoformat()))

        rows = cur.fetchall()
        conn.close()

        for pid, play_ts, station_show, title, source_url in rows:
            if (
                title.endswith("W/or")
                or title.count("(") != title.count(")")
                or len(title.strip()) < 3
            ):
                msg = (
                    f"suspicious_title id={pid} "
                    f"play_ts={play_ts} "
                    f"station_show='{station_show}' "
                    f"title='{title}' "
                    f"url='{source_url}' "
                )
                logging.warning(msg)
                anomalies.append(msg)

    # ---------------- DATABASE SNAPSHOT ----------------

    conn = sqlite3.connect("radio_plays.db")
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM plays")
    total_plays = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM canonical_tracks")
    total_canonicals = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM plays_to_canonical")
    total_mapped = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM canonical_tracks WHERE spotify_album_id IS NOT NULL")
    total_enriched = cur.fetchone()[0]

    conn.close()

    # ---------------- SUMMARY LOGGING ----------------

    logging.info("---- DAILY SCRAPE SUMMARY ----")
    logging.info(f"first_hour={first_hour} last_hour={last_hour}")
    logging.info(f"pages_attempted={pages_attempted} pages_fetched={pages_fetched}")
    logging.info(f"plays_seen={total_seen} plays_inserted={total_inserted}")

    logging.info(
        f"METRIC db_state plays={total_plays} "
        f"canonicals={total_canonicals} "
        f"mapped={total_mapped} "
        f"enriched={total_enriched}"
    )

    if anomalies:
        logging.warning("ATTENTION REQUIRED")
        for a in anomalies:
            logging.warning(a)
    else:
        logging.info("No anomalies detected.")

    return {
        "plays_seen": total_seen,
        "plays_inserted": total_inserted
    }


def run_backfill(start_iso: str, end_iso: str):
    ny_tz = ZoneInfo("America/New_York")

    start_dt = datetime.fromisoformat(start_iso).replace(tzinfo=ny_tz)
    end_dt = datetime.fromisoformat(end_iso).replace(tzinfo=ny_tz)

    if start_dt > end_dt:
        print("Start must be before end.")
        return

    print("Backfill window:")
    print("  Start:", start_dt)
    print("  End:  ", end_dt)

    total_seen = 0
    total_inserted = 0

    current_dt = start_dt.replace(minute=0, second=0, microsecond=0)

    while current_dt <= end_dt:
        url = build_play_url(current_dt.date(), current_dt.hour)
        print(f"Fetching {url}")

        try:
            html = fetch_url(url)
        except Exception as e:
            print(f"Failed to fetch {url}: {e}")
            current_dt += timedelta(hours=1)
            continue

        plays = parse_played_page(html, url)

        total_seen += len(plays)

        inserted = 0
        for p in plays:
            if insert_play(p):
                inserted += 1

        total_inserted += inserted

        print(f"  Parsed: {len(plays)} | Inserted: {inserted}")

        time.sleep(CRAWL_DELAY)

        current_dt += timedelta(hours=1)

    print("\n--- Backfill Summary ---")
    print({
        "plays_seen": total_seen,
        "plays_inserted": total_inserted
    })

    return {
        "plays_seen": total_seen,
        "plays_inserted": total_inserted
    }