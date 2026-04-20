"""
Daily Collection Runner
=======================
Entry point for the scheduled data collection job.

Runs every day at 6 AM (configurable), pulls the last 7 days of
ad metrics from Meta, enriches with creative details, and saves
everything to SQLite ready for the analyzer.

Deploy options:
  - python run_collector.py                  (run once immediately)
  - python run_collector.py --schedule       (run on cron loop)
  - crontab: 0 6 * * * python run_collector.py
  - GitHub Actions: schedule trigger (see README)
"""

import argparse
import logging
import schedule
import time
import json
from datetime import datetime
from data_collector.collector import MetaCollector
from data_collector.creative_fetcher import fetch_creatives_bulk

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("collector.log"),
    ],
)
log = logging.getLogger("runner")


def run_collection(days: int = 7, fetch_creatives: bool = True):
    """Full collection pipeline: metrics + optional creative enrichment."""
    log.info("=" * 60)
    log.info("Collection run — %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    log.info("=" * 60)

    collector = MetaCollector()

    # 1. Pull all ad metrics
    metrics = collector.collect_all(days=days)
    log.info("Metrics collected: %d ads", len(metrics))

    # 2. Enrich with creative details (format, thumbnail, headline, CTA)
    if fetch_creatives and metrics:
        ad_ids = list({m["ad_id"] for m in metrics if m.get("ad_id")})
        log.info("Fetching creative details for %d unique ads...", len(ad_ids))
        creatives = fetch_creatives_bulk(ad_ids)

        # Merge creative data into metric rows
        creative_map = {c["ad_id"]: c for c in creatives.values()}
        for m in metrics:
            ad_id = m.get("ad_id")
            if ad_id in creative_map:
                m.update(creative_map[ad_id])

        log.info("Creative enrichment complete")

    # 3. Print summary to log
    summary = collector.summary(days=days)
    log.info("Account summary (last %d days):", days)
    log.info("  Ads tracked:      %s", summary.get("ads"))
    log.info("  Total spend:      $%s", summary.get("total_spend"))
    log.info("  Total revenue:    $%s", summary.get("total_revenue"))
    log.info("  Overall ROAS:     %sx", summary.get("overall_roas"))
    log.info("  Avg CTR:          %s%%", round((summary.get("avg_ctr") or 0) * 100, 2))
    log.info("  Avg CPM:          $%s", summary.get("avg_cpm"))

    winners = collector.winners(days=days)
    losers  = collector.losers(days=days)
    pending = collector.needs_more_data(days=days)
    log.info("  Winners (ROAS≥2): %d ads", len(winners))
    log.info("  Losers  (ROAS≤.5): %d ads", len(losers))
    log.info("  Needs more data:  %d ads", len(pending))

    if winners:
        log.info("Top winner: %s — ROAS %sx, spend $%s",
                 winners[0]["ad_name"], winners[0]["roas"], winners[0]["spend"])

    log.info("Collection run complete.\n")
    return metrics


def main():
    parser = argparse.ArgumentParser(description="Meta Ads data collector")
    parser.add_argument("--days",     type=int, default=7,    help="Lookback window in days")
    parser.add_argument("--schedule", action="store_true",    help="Run on daily schedule")
    parser.add_argument("--no-creative", action="store_true", help="Skip creative enrichment")
    parser.add_argument("--summary",  action="store_true",    help="Print DB summary and exit")
    args = parser.parse_args()

    if args.summary:
        c = MetaCollector()
        s = c.summary(days=args.days)
        print(json.dumps(s, indent=2))
        return

    fetch_creatives = not args.no_creative

    if args.schedule:
        log.info("Scheduler started — runs daily at 06:00")
        schedule.every().day.at("06:00").do(run_collection, days=args.days, fetch_creatives=fetch_creatives)
        # Also run immediately on startup
        run_collection(days=args.days, fetch_creatives=fetch_creatives)
        while True:
            schedule.run_pending()
            time.sleep(60)
    else:
        run_collection(days=args.days, fetch_creatives=fetch_creatives)


if __name__ == "__main__":
    main()
