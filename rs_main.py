import argparse
from scraper.orchestrator import run_scrape, run_backfill
from scraper.weekly import run_weekly


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
        summary = run_scrape()
        print("\n--- Summary ---")
        print(summary)

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