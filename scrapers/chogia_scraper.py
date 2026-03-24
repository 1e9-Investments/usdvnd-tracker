"""
chogia.vn Scraper
Grey market rates - 30-day rolling history via AJAX API.
"""

import requests
from datetime import datetime
import time
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from database import upsert_rate, log_scrape, compute_grey_premium

API_URL = "https://chogia.vn/wp-admin/admin-ajax.php"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Content-Type": "application/x-www-form-urlencoded",
    "X-Requested-With": "XMLHttpRequest"
}


def fetch_history() -> list:
    """
    Fetch 30-day USD grey market history.
    Returns list of {date, buy, sell} records.
    """
    data = {
        "action": "load_gia_ngoai_te_cho_do_thi",
        "ma": "usd"
    }

    try:
        resp = requests.post(API_URL, data=data, headers=HEADERS, timeout=30)
        resp.raise_for_status()

        result = resp.json()

        # Response format varies - could be direct list or nested
        if isinstance(result, list):
            return result
        elif isinstance(result, dict):
            return result.get("data", []) or result.get("history", [])
        else:
            return []

    except requests.RequestException as e:
        print(f"Request error: {e}")
        return []
    except ValueError as e:
        print(f"JSON parse error: {e}")
        return []


def parse_rate(value) -> float:
    """Parse rate from various formats."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.replace(',', '').replace(' ', '').replace('.', '')
        # Handle case where last 3 digits might be decimals
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def import_history():
    """Fetch and import available history (30 days)."""
    print("Fetching 30-day history from chogia.vn...")
    start_time = time.time()

    records = fetch_history()

    if not records:
        log_scrape("chogia.vn", "30d", "error", 0, "No data returned", time.time() - start_time)
        print("No records returned")
        return 0

    imported = 0
    for record in records:
        try:
            # Parse date
            date_val = record.get("date") or record.get("ngay") or record.get("time") or record.get("label")
            if not date_val:
                continue

            # Try various date formats
            iso_date = None
            if isinstance(date_val, str):
                for fmt in ["%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%d/%m"]:
                    try:
                        dt = datetime.strptime(date_val, fmt)
                        # Handle year-less format
                        if fmt == "%d/%m":
                            dt = dt.replace(year=datetime.now().year)
                        iso_date = dt.strftime("%Y-%m-%d")
                        break
                    except ValueError:
                        continue

            if not iso_date:
                continue

            # Extract rates
            buy = parse_rate(record.get("buy") or record.get("mua") or record.get("gia_mua"))
            sell = parse_rate(record.get("sell") or record.get("ban") or record.get("gia_ban"))

            # Sometimes data comes as single value
            if not buy and not sell:
                val = parse_rate(record.get("value") or record.get("gia"))
                if val:
                    sell = val  # Assume it's the sell rate

            if buy or sell:
                upsert_rate(iso_date, {
                    "chogia_buy": buy,
                    "chogia_sell": sell
                })
                compute_grey_premium(iso_date)
                imported += 1

        except Exception as e:
            print(f"Error processing record {record}: {e}")
            continue

    duration = time.time() - start_time
    log_scrape("chogia.vn", "30d", "success", imported, None, duration)
    print(f"Imported {imported} records in {duration:.1f}s")
    return imported


def scrape_today():
    """Fetch current grey market rate."""
    print("Fetching current rates from chogia.vn...")
    start_time = time.time()

    # Fetch history and get most recent
    records = fetch_history()

    if not records:
        today = datetime.now().strftime("%Y-%m-%d")
        log_scrape("chogia.vn", today, "error", 0, "No data", time.time() - start_time)
        return None

    # Get most recent record (usually last in list)
    latest = records[-1] if records else None

    if latest:
        buy = parse_rate(latest.get("buy") or latest.get("mua") or latest.get("gia_mua"))
        sell = parse_rate(latest.get("sell") or latest.get("ban") or latest.get("gia_ban"))

        today = datetime.now().strftime("%Y-%m-%d")

        if buy or sell:
            data = {"chogia_buy": buy, "chogia_sell": sell}
            upsert_rate(today, data)
            compute_grey_premium(today)
            log_scrape("chogia.vn", today, "success", 1, None, time.time() - start_time)
            print(f"Latest rate: buy={buy}, sell={sell}")
            return data

    return None


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="chogia.vn scraper")
    parser.add_argument("--backfill", action="store_true", help="Import 30-day history")
    parser.add_argument("--today", action="store_true", help="Fetch latest rate")

    args = parser.parse_args()

    if args.backfill:
        import_history()
    elif args.today:
        scrape_today()
    else:
        # Default: import available history
        import_history()
