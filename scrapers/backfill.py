#!/usr/bin/env python3
"""
Backfill Script
Runs all available historical data imports.
"""

import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from database import get_date_range, export_to_csv


def run_backfill(years: int = 5, skip_tygiausd: bool = False):
    """
    Run full backfill for all sources.

    Args:
        years: Number of years to backfill (for tygiausd.org)
        skip_tygiausd: Skip the long tygiausd.org scrape
    """
    print("=" * 60)
    print(f"USDVND Historical Backfill - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    start_time = time.time()

    # 1. CoinGecko - quick API call
    print("\n[1/4] CoinGecko USDT/VND (1 year available)...")
    try:
        from coingecko_scraper import import_history
        import_history(years=1)
    except Exception as e:
        print(f"Error: {e}")

    time.sleep(2)

    # 2. tygiachoden.com - quick API call
    print("\n[2/4] tygiachoden.com (1 year)...")
    try:
        from tygiachoden_scraper import import_history
        import_history("1year")
    except Exception as e:
        print(f"Error: {e}")

    time.sleep(2)

    # 3. chogia.vn - quick API call
    print("\n[3/4] chogia.vn (30 days)...")
    try:
        from chogia_scraper import import_history
        import_history()
    except Exception as e:
        print(f"Error: {e}")

    time.sleep(2)

    # 4. tygiausd.org - LONG scrape (5 years = ~1800 requests)
    if not skip_tygiausd:
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=years * 365)).strftime("%Y-%m-%d")

        print(f"\n[4/4] tygiausd.org ({years} years: {start_date} to {end_date})...")
        print("This will take approximately 45-90 minutes...")

        try:
            from tygiausd_scraper import backfill
            backfill(start_date, end_date, delay=1.5)
        except Exception as e:
            print(f"Error: {e}")
    else:
        print("\n[4/4] tygiausd.org - SKIPPED (use --full to include)")

    # Summary
    duration = time.time() - start_time
    print("\n" + "=" * 60)
    print("BACKFILL COMPLETE")
    print("=" * 60)
    print(f"Duration: {duration:.1f}s ({duration/60:.1f} minutes)")

    # Show database stats
    min_date, max_date, count = get_date_range()
    print(f"Database: {count} records from {min_date} to {max_date}")

    # Export to CSV
    print("\nExporting to CSV...")
    export_to_csv()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="USDVND Historical Backfill")
    parser.add_argument("--years", type=int, default=5, help="Years to backfill")
    parser.add_argument("--quick", action="store_true",
                        help="Quick mode - skip tygiausd.org (fast APIs only)")
    parser.add_argument("--full", action="store_true",
                        help="Full mode - include tygiausd.org 5-year scrape")

    args = parser.parse_args()

    skip_tygiausd = args.quick or not args.full
    run_backfill(years=args.years, skip_tygiausd=skip_tygiausd)
