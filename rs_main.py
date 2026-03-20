import argparse
from scraper.orchestrator import run_scrape, run_backfill
from scraper.weekly import run_weekly
from scraper.normalization import normalize_new_plays
from scraper.canonical import seed_new_canonicals, map_new_plays
from scraper.audit import run_full_audit
from analytics.analysis import run_analysis
import sys
import logging


def run_normalize():
    return normalize_new_plays()

def run_seed_canonicals():
    return seed_new_canonicals()

def run_map_plays():
    return map_new_plays()

def run_ingest():
    return run_scrape()


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

        # 5. Post-pipeline audit
        run_full_audit()

        return summary

    elif args.mode == "weekly":
        run_weekly()

    elif args.mode == "analyze":
        run_analysis()

    elif args.mode == "backfill":
        if not args.start or not args.end:
            print("Backfill requires --start and --end in ISO format (YYYY-MM-DDTHH:MM)")
            return

        run_backfill(args.start, args.end)

    elif args.mode == "audit":
        run_full_audit()


if __name__ == "__main__":
    try:
        main()

    except Exception as e:
        logging.exception("Fatal unhandled error occurred")

        sys.exit(1)
