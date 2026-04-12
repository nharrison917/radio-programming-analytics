import argparse
from scraper.orchestrator import run_scrape, run_backfill
from scraper.weekly import run_weekly
from scraper.normalization import normalize_new_plays
from scraper.canonical import seed_new_canonicals, map_new_plays
from scraper.audit import run_full_audit
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
        choices=["scrape", "weekly", "analyze", "cluster", "backfill", "audit",
                 "enrich-meta", "mb-enrich", "add-override", "set-meta"],
        help="Which job to run"
    )
    parser.add_argument("--start", type=str, help="ISO start datetime (YYYY-MM-DDTHH:MM)")
    parser.add_argument("--end",   type=str, help="ISO end datetime (YYYY-MM-DDTHH:MM)")
    parser.add_argument("--id",         type=int, help="canonical_id (add-override, set-meta)")
    parser.add_argument("--spotify-id", type=str, help="Spotify track ID (add-override)")
    parser.add_argument("--year",       type=str, help="Year override: YYYY or YYYY-MM-DD (set-meta)")
    parser.add_argument("--duration",   type=str, help="Duration override: M:SS (set-meta)")

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
        from analytics.analysis import run_analysis
        from analytics.era_continuity import run_era_continuity
        from analytics.boxplot_release_year import build_release_year_boxplot
        from analytics.heatmap_weekly_density import run_heatmap_weekly_density
        from analytics.heatmap_avg_release_year import run_heatmap_avg_release_year
        from analytics.wednesday_freshness import run_wednesday_freshness
        from analytics.segment_breakers import run_segment_breakers

        run_analysis()
        run_era_continuity()
        build_release_year_boxplot()
        run_heatmap_weekly_density()
        run_heatmap_avg_release_year()
        run_wednesday_freshness()
        run_segment_breakers()

    elif args.mode == "cluster":
        from analytics.show_clustering import run_show_clustering
        run_show_clustering()

    elif args.mode == "backfill":
        if not args.start or not args.end:
            print("Backfill requires --start and --end in ISO format (YYYY-MM-DDTHH:MM)")
            return

        run_backfill(args.start, args.end)

    elif args.mode == "enrich-meta":
        from scraper.spotify_backfill import backfill_spotify_meta
        backfill_spotify_meta()

    elif args.mode == "mb-enrich":
        from scraper.mb_enrichment import run_mb_enrichment
        run_mb_enrichment()

    elif args.mode == "audit":
        run_full_audit()

    elif args.mode == "add-override":
        if not args.id or not args.spotify_id:
            print("add-override requires --id <canonical_id> and --spotify-id <spotify_id>")
            return
        from scraper.overrides import run_add_override
        run_add_override(args.id, args.spotify_id)

    elif args.mode == "set-meta":
        if not args.id:
            print("set-meta requires --id <canonical_id>")
            return
        from scraper.overrides import run_set_meta
        run_set_meta(args.id, year_raw=args.year, duration_raw=args.duration)


if __name__ == "__main__":
    try:
        main()

    except Exception as e:
        logging.exception("Fatal unhandled error occurred")

        sys.exit(1)
