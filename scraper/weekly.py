from scraper.enrichment import enrich_all
from scraper.config import SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET, DB_PATH
from scraper.utils import create_backup, setup_logging, rotate_backups, rotate_logs
import logging
from pathlib import Path
import pandas as pd
import sqlite3




def run_enrich_spotify():
    result = enrich_all(SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET)
    return result


def run_weekly():

    print("Starting weekly pipeline...\n")

    setup_logging("weekly")
    logging.info("Starting weekly pipeline")

    # ---------------------
    # Backup
    # ---------------------
    create_backup()
    backup_dir = Path("backups")
    rotate_backups(backup_dir, max_backups=10)

    summary = {}

    # ---------------------
    # Enrichment
    # ---------------------
    enrichment_summary = run_enrich_spotify() or {}

    summary.update({
        "enriched": enrichment_summary.get("enriched", 0),
        "failures": enrichment_summary.get("failures", 0),
        "new_failures": enrichment_summary.get("new_failures", 0),
        "abort": enrichment_summary.get("rate_limit_abort", False),
    })

    attempt_counts = enrichment_summary.get("attempt_counts", {})

    logging.info("---- WEEKLY ENRICHMENT SUMMARY ----")
    logging.info(
        f"enriched={summary['enriched']} "
        f"failures={summary['failures']} "
        f"new_failures={summary['new_failures']} "
        f"abort={summary['abort']}"
    )
    if summary["new_failures"] > 0:
        logging.warning(
            f"ATTENTION: {summary['new_failures']} track(s) failed Spotify enrichment "
            f"for the first time this run - check enrichment_failures.csv"
        )

    logging.info(
        "attempt_distribution "
        + " ".join(f"attempt{a}={attempt_counts.get(a,0)}" for a in [1,2,3,4])
    )

    # ---------------------
    # Weekly Reports (AFTER enrichment)
    # ---------------------
    weekly_output_dir = Path("analytics/outputs/weekly_reports")
    weekly_output_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)

    # Attempt 3 & 4 successes
    df_attempt_34 = pd.read_sql_query("""
        SELECT
            canonical_id,
            norm_artist,
            display_title,
            spotify_match_attempt,
            spotify_album_release_year,
            spotify_title_score,
            spotify_artist_score,
            spotify_enriched_at
        FROM canonical_tracks
        WHERE spotify_status = 'SUCCESS'
          AND spotify_match_attempt IN (3, 4)
    """, conn)

    df_attempt_34.to_csv(
        weekly_output_dir / "enrichment_attempt_3_4.csv",
        index=False
    )

    # Failures
    df_failures = pd.read_sql_query("""
        SELECT
            canonical_id,
            norm_artist,
            display_title,
            spotify_attempt_count,
            spotify_status,
            spotify_last_attempted_at
        FROM canonical_tracks
        WHERE spotify_attempt_count > 0
          AND spotify_status != 'SUCCESS'
        ORDER BY spotify_attempt_count DESC
    """, conn)

    df_failures.to_csv(
        weekly_output_dir / "enrichment_failures.csv",
        index=False
    )

    # Optional health snapshot
    success_count = pd.read_sql_query("""
        SELECT COUNT(*) AS count
        FROM canonical_tracks
        WHERE spotify_status = 'SUCCESS'
    """, conn)["count"][0]

    failure_count = pd.read_sql_query("""
        SELECT COUNT(*) AS count
        FROM canonical_tracks
        WHERE spotify_status != 'SUCCESS'
    """, conn)["count"][0]

    # ---------------------
    # Enrichment State Snapshot
    # ---------------------
    state_counts = pd.read_sql_query("""
        SELECT spotify_status, COUNT(*) AS count
        FROM canonical_tracks
        GROUP BY spotify_status
    """, conn)

    logging.info("---- ENRICHMENT STATE SNAPSHOT ----")

    for _, row in state_counts.iterrows():
        logging.info(f"  {row['spotify_status']}: {row['count']}")

    total = state_counts["count"].sum()
    logging.info(f"  TOTAL canonicals: {total}")

    conn.close()

    # ---------------------
    # Rotate logs LAST
    # ---------------------
    log_dir = Path("logs")
    rotate_logs(log_dir, prefix="weekly", max_logs=15)

    print("\n--- Weekly Summary ---")
    print(summary)

    return summary