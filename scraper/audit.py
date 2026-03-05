import sqlite3
import logging
from scraper.config import (
    MIN_PLAYS_PER_HOUR,
    MAX_PLAYS_PER_HOUR,
    FLAG_NULL_STATION_SHOW,
    FLAG_SUSPICIOUS_TITLE,
)
from scraper.utils import setup_logging


def run_full_audit():
    setup_logging("audit")
    logging.info("Starting full database audit")

    conn = sqlite3.connect("radio_plays.db")
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

    conn.close()

    logging.info("---- AUDIT SUMMARY ----")
    logging.info(f"total_anomalies={len(anomalies)}")

    if anomalies:
        print("\n⚠️ AUDIT ATTENTION REQUIRED:")
        for a in anomalies[:20]:
            print("-", a)
        if len(anomalies) > 20:
            print(f"... and {len(anomalies) - 20} more")
    else:
        print("\nNo anomalies detected in full audit.")