"""
CoinGecko Scraper
USDT/VND rates - this represents USDT priced at official VND conversion.
Used as baseline for computing grey market premium.
"""

import requests
from datetime import datetime, timedelta
import time
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from database import upsert_rate, log_scrape

# CoinGecko API - free tier
API_URL = "https://api.coingecko.com/api/v3"
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json"
}


def fetch_market_chart(days: int = 365) -> list:
    """
    Fetch USDT/VND price history.
    Free API limited to 365 days per request.
    Returns list of [timestamp_ms, price] pairs.
    """
    url = f"{API_URL}/coins/tether/market_chart"
    params = {
        "vs_currency": "vnd",
        "days": days,
        "interval": "daily"
    }

    try:
        resp = requests.get(url, params=params, headers=HEADERS, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        return data.get("prices", [])
    except requests.RequestException as e:
        print(f"Request error: {e}")
        return []


def import_history(years: int = 5):
    """
    Import historical USDT/VND data.
    CoinGecko free API limits to 365 days, so we need multiple calls.
    """
    print(f"Fetching {years} years of USDT/VND history from CoinGecko...")
    start_time = time.time()
    total_imported = 0

    # Free API can only do 365 days at a time
    # For 5 years, we'd need a paid plan or work around it
    # Let's try max available first

    for year_offset in range(years):
        # CoinGecko free tier only gives 365 days
        # For historical data beyond that, we'd need CoinGecko Pro
        # For now, fetch what's available
        if year_offset == 0:
            days = 365
        else:
            print(f"Note: CoinGecko free tier limited to 365 days. Year {year_offset + 1} skipped.")
            continue

        print(f"Fetching last {days} days...")
        prices = fetch_market_chart(days)

        if not prices:
            print("No data returned")
            continue

        imported = 0
        for timestamp_ms, price in prices:
            try:
                dt = datetime.fromtimestamp(timestamp_ms / 1000)
                iso_date = dt.strftime("%Y-%m-%d")

                upsert_rate(iso_date, {
                    "coingecko_usdt_vnd": round(price, 2)
                })
                imported += 1
            except Exception as e:
                print(f"Error: {e}")
                continue

        print(f"Imported {imported} records")
        total_imported += imported

        # Rate limit pause between requests
        if year_offset < years - 1:
            time.sleep(2)

    duration = time.time() - start_time
    log_scrape("coingecko", f"{years}y", "success", total_imported, None, duration)
    print(f"Total imported: {total_imported} records in {duration:.1f}s")
    return total_imported


def scrape_today():
    """Fetch latest USDT/VND rate."""
    print("Fetching latest USDT/VND from CoinGecko...")
    start_time = time.time()

    url = f"{API_URL}/simple/price"
    params = {
        "ids": "tether",
        "vs_currencies": "vnd"
    }

    try:
        resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        price = data.get("tether", {}).get("vnd")
        if price:
            today = datetime.now().strftime("%Y-%m-%d")
            upsert_rate(today, {"coingecko_usdt_vnd": round(price, 2)})
            log_scrape("coingecko", today, "success", 1, None, time.time() - start_time)
            print(f"USDT/VND: {price:,.0f}")
            return price
        else:
            log_scrape("coingecko", datetime.now().strftime("%Y-%m-%d"), "error", 0, "No price", time.time() - start_time)
            return None

    except requests.RequestException as e:
        print(f"Error: {e}")
        log_scrape("coingecko", datetime.now().strftime("%Y-%m-%d"), "error", 0, str(e), time.time() - start_time)
        return None


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="CoinGecko USDT/VND scraper")
    parser.add_argument("--backfill", action="store_true", help="Import available history")
    parser.add_argument("--years", type=int, default=5, help="Years to fetch (limited by API)")
    parser.add_argument("--today", action="store_true", help="Fetch latest rate")

    args = parser.parse_args()

    if args.backfill:
        import_history(args.years)
    elif args.today:
        scrape_today()
    else:
        # Default: fetch available history
        import_history(1)
