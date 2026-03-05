from scraper.canonical import seed_new_canonicals, map_new_plays
from scraper.enrichment import enrich_all
from scraper.config import SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET
from scraper.utils import create_backup, setup_logging
import logging

def run_normalize():
    print("Running normalization job...")
    from scraper.normalization import normalize_new_plays
    return normalize_new_plays()


def run_seed_canonicals():
    return seed_new_canonicals()


def run_map_plays():
    return map_new_plays()


def run_enrich_spotify():
    result = enrich_all(SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET)
    return result


def run_weekly():
    print("Starting weekly pipeline...\n")
    setup_logging("weekly")
    logging.info("Starting weekly pipeline")

    create_backup()
    summary = {}

    # 1. Normalize new plays
    summary.update(run_normalize())
    
    # 2. Seed new canonicals
    summary.update(run_seed_canonicals())
    
    # 3. Map new canonicals
    summary.update(run_map_plays())

    # 4. Enrich new canonicals
    enrichment_summary = run_enrich_spotify() or {}
    
    summary.update({
        "enriched": enrichment_summary.get("enriched", 0),
        "failures": enrichment_summary.get("failures",0),
        "abort": enrichment_summary.get("rate_limit_abort", False),
    })

    attempt_counts = enrichment_summary.get("attempt_counts", {})

    logging.info("----WEEKLY ENRICHMENT SUMMARY ----")
    logging.info(
        f"enriched={summary['enriched']} "
        f"failures={summary['failures']} "
        f"abort={summary['abort']}"
    )

    logging.info(
    "attempt_distribution "
    + " ".join(f"attempt{a}={attempt_counts.get(a,0)}" for a in [1,2,3,4])
    )

    print("\n--- Weekly Summary ---")
    print(summary)

    return summary