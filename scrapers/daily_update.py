#!/usr/bin/env python3
"""
Daily Update Orchestrator
Runs all scrapers and updates the database with latest rates.
"""

import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from database import get_date_range, export_to_csv
from tygiausd_scraper import scrape_today as scrape_tygiausd
from tygiachoden_scraper import scrape_today as scrape_tygiachoden
from coingecko_scraper import scrape_today as scrape_coingecko
from binance_p2p_scraper import scrape_current as scrape_binance
from chogia_scraper import scrape_today as scrape_chogia
from official_rate_scraper import scrape_today as scrape_official


def run_daily_update():
    """Run all scrapers for daily update."""
    print("=" * 60)
    print(f"USDVND Daily Update - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    results = {}
    start_time = time.time()

    # 1. Official rate (baseline)
    print("\n[1/6] Official USD/VND rate...")
    try:
        results['official'] = scrape_official()
    except Exception as e:
        print(f"Error: {e}")
        results['official'] = None

    # 2. tygiausd.org (primary grey market)
    print("\n[2/6] tygiausd.org (grey market)...")
    try:
        results['tygiausd'] = scrape_tygiausd()
    except Exception as e:
        print(f"Error: {e}")
        results['tygiausd'] = None

    time.sleep(1)

    # 3. tygiachoden.com
    print("\n[3/6] tygiachoden.com...")
    try:
        results['tygiachoden'] = scrape_tygiachoden()
    except Exception as e:
        print(f"Error: {e}")
        results['tygiachoden'] = None

    time.sleep(1)

    # 4. chogia.vn
    print("\n[4/6] chogia.vn...")
    try:
        results['chogia'] = scrape_chogia()
    except Exception as e:
        print(f"Error: {e}")
        results['chogia'] = None

    time.sleep(1)

    # 5. Binance P2P
    print("\n[5/6] Binance P2P USDT/VND...")
    try:
        results['binance'] = scrape_binance()
    except Exception as e:
        print(f"Error: {e}")
        results['binance'] = None

    time.sleep(1)

    # 6. CoinGecko
    print("\n[6/6] CoinGecko USDT/VND...")
    try:
        results['coingecko'] = scrape_coingecko()
    except Exception as e:
        print(f"Error: {e}")
        results['coingecko'] = None

    # Summary
    duration = time.time() - start_time
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    success_count = sum(1 for v in results.values() if v is not None)
    print(f"Sources updated: {success_count}/6")
    print(f"Duration: {duration:.1f}s")

    # Show database stats
    min_date, max_date, count = get_date_range()
    print(f"\nDatabase: {count} records from {min_date} to {max_date}")

    # Export to CSV
    print("\nExporting to CSV...")
    export_to_csv()

    return results


if __name__ == "__main__":
    run_daily_update()
