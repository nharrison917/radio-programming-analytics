import argparse
from scraper.orchestrator import run_scrape, run_backfill
from scraper.weekly import run_weekly
from scraper.normalization import normalize_new_plays
from scraper.canonical import seed_new_canonicals, map_new_plays
import sqlite3
from pathlib import Path

def run_normalize():    
    return normalize_new_plays()

def run_seed_canonicals():
    return seed_new_canonicals()

def run_map_plays():
    return map_new_plays()

def run_ingest():
    return run_scrape()


def get_existing_station_shows(db_path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT station_show
        FROM plays
        WHERE station_show IS NOT NULL;
    """)
    rows = cur.fetchall()
    conn.close()
    return {r[0] for r in rows}


def audit_station_shows(db_path, existing_shows):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # NULL station_show entries
    cur.execute("""
        SELECT id, source_url
        FROM plays
        WHERE station_show IS NULL
        ORDER BY id DESC;
    """)
    null_rows = cur.fetchall()

    # Current distinct shows
    cur.execute("""
        SELECT DISTINCT station_show
        FROM plays
        WHERE station_show IS NOT NULL;
    """)
    current_shows = {r[0] for r in cur.fetchall()}

    conn.close()

    new_shows = current_shows - existing_shows

    return null_rows, new_shows


def audit_quality_metrics(db_path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # NULL metadata after normalization
    cur.execute("""
        SELECT COUNT(*)
        FROM canonical_tracks
        WHERE norm_artist IS NULL
           OR norm_title_core IS NULL;
    """)
    null_meta_count = cur.fetchone()[0]

    # Low confidence mappings
    cur.execute("""
        SELECT COUNT(*)
        FROM plays
        WHERE confidence != 'high'
          AND station_show_confidence NOT IN ('medium', 'high');
    """)
    low_conf_count = cur.fetchone()[0]

    conn.close()

    return null_meta_count, low_conf_count



def get_existing_station_shows(db_path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT station_show
        FROM plays
        WHERE station_show IS NOT NULL;
    """)
    rows = cur.fetchall()
    conn.close()
    return {r[0] for r in rows}


def audit_station_shows(db_path, existing_shows):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # NULL station_show entries
    cur.execute("""
        SELECT id, source_url
        FROM plays
        WHERE station_show IS NULL
        ORDER BY id DESC;
    """)
    null_rows = cur.fetchall()

    # Current distinct shows
    cur.execute("""
        SELECT DISTINCT station_show
        FROM plays
        WHERE station_show IS NOT NULL;
    """)
    current_shows = {r[0] for r in cur.fetchall()}

    conn.close()

    new_shows = current_shows - existing_shows

    return null_rows, new_shows


def audit_quality_metrics(db_path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # NULL metadata after normalization
    cur.execute("""
        SELECT COUNT(*)
        FROM canonical_tracks
        WHERE norm_artist IS NULL
           OR norm_title_core IS NULL;
    """)
    null_meta_count = cur.fetchone()[0]

    # Low confidence mappings
    cur.execute("""
        SELECT COUNT(*)
        FROM plays
        WHERE confidence != 'high'
          AND station_show_confidence NOT IN ('medium', 'high');
    """)
    low_conf_count = cur.fetchone()[0]

    conn.close()

    return null_meta_count, low_conf_count

def main():
    parser = argparse.ArgumentParser(description="Radio Scraper CLI")

    parser.add_argument(
        "mode",
        choices=["scrape", "weekly", "analyze", "backfill", "audit"],
        help="Which job to run"
    )
    parser.add_argument("--start", type=str, help="ISO start datetime (YYYY-MM-DDTHH:MM)")
    parser.add_argument("--end", type=str, help="ISO end datetime (YYYY-MM-DDTHH:MM)")

    args = parser.parse_args()

    if args.mode == "scrape":
        summary = {}

        # 1. Scrape / ingest new plays
        summary.update(run_ingest())

        # 2. Normalize new plays
        summary.update(run_normalize())

        # 3. Seed new canonicals
        summary.update(run_seed_canonicals())

        # 4. Map plays to canonicals
        summary.update(run_map_plays())

        def audit_station_shows(db_path, existing_shows):
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()

            # NULL station_show entries (only newly inserted rows)
            cur.execute("""
                SELECT id, source_url
                FROM plays
                WHERE station_show IS NULL
                ORDER BY id DESC
                LIMIT 20;
            """)
            null_rows = cur.fetchall()

            # Current distinct shows
            cur.execute("""
                SELECT DISTINCT station_show
                FROM plays
                WHERE station_show IS NOT NULL;
            """)
            current_shows = {r[0] for r in cur.fetchall()}

            conn.close()

            new_shows = current_shows - existing_shows

            return null_rows, new_shows
        
            null_rows, new_shows = audit_station_shows(DB_PATH, existing_shows)

            logger.info("Station Show Audit")

            if null_rows:
                logger.info(f"NULL station_show entries: {len(null_rows)}")
            for row in null_rows:
                logger.info(f"  play_id={row[0]}, source={row[1]}")
            else:
                logger.info("No NULL station_show entries detected.")

            if new_shows:
                logger.info("New station_show detected:")
                for show in sorted(new_shows):
                    logger.info(f"  {show}")
            else:
                logger.info("No new station_show detected.")

        return summary

    elif args.mode == "weekly":
        run_weekly()

    elif args.mode == "analyze":
        print("Analysis pipeline not implemented yet.")
        # Placeholder for analysis logic

    elif args.mode == "backfill":
        if not args.start or not args.end:
            print("Backfill requires --start and --end in ISO format (YYYY-MM-DDTHH:MM)")
            return

        run_backfill(args.start, args.end)

    elif args.mode == "audit":
        from scraper.audit import run_full_audit
        run_full_audit()


if __name__ == "__main__":
    main()