import sqlite3
import logging
from scraper.config import (
    FLAG_NULL_STATION_SHOW,
    FLAG_SUSPICIOUS_TITLE,
    DB_PATH,
)
from scraper.utils import setup_logging, rotate_logs
from pathlib import Path


def run_full_audit():
    setup_logging("audit")
    logging.info("Starting full database audit")

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    anomalies = []

    # --- Suspicious Titles ---
    if FLAG_SUSPICIOUS_TITLE:
        cur.execute("""
            SELECT id, play_ts, station_show, title, source_url
            FROM plays
        """)
        rows = cur.fetchall()

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
                    f"url='{source_url}'"
                )
                logging.warning(msg)
                anomalies.append(msg)

    # --- NULL station_show ---
    if FLAG_NULL_STATION_SHOW:
        cur.execute("""
            SELECT COUNT(*) FROM plays
            WHERE station_show IS NULL
        """)
        null_count = cur.fetchone()[0]

        if null_count > 0:
            msg = f"station_show_null count={null_count}"
            logging.warning(msg)
            anomalies.append(msg)

    # --- Unenriched canonicals ---
    cur.execute("""
        SELECT spotify_status, COUNT(*) as count
        FROM canonical_tracks
        WHERE spotify_status != 'SUCCESS'
        GROUP BY spotify_status
        ORDER BY count DESC
    """)
    unenriched = cur.fetchall()

    total_unenriched = sum(r[1] for r in unenriched)

    if total_unenriched > 0:
        msg = f"unenriched_canonicals total={total_unenriched}"
        logging.warning(msg)
        anomalies.append(msg)
        for status, count in unenriched:
            logging.warning(f"  {status}: {count}")
    else:
        logging.info("All canonicals successfully enriched.")

    conn.close()

    logging.info("---- AUDIT SUMMARY ----")
    logging.info(f"total_anomalies={len(anomalies)}")

    log_dir = Path("logs")
    rotate_logs(log_dir, prefix="audit", max_logs=5)

    if anomalies:
        print("\n*** AUDIT ATTENTION REQUIRED ***")
        for a in anomalies[:20]:
            print("-", a)
        if len(anomalies) > 20:
            print(f"... and {len(anomalies) - 20} more")
    else:
        print("\nNo anomalies detected in full audit.")