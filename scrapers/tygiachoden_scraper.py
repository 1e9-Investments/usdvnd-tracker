"""
tygiachoden.com Scraper
Grey market rates with ~1 year history via JSON API.
"""

import requests
from datetime import datetime
import time
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from database import upsert_rate, log_scrape, compute_grey_premium

API_URL = "https://tygiachoden.com/wp-admin/admin-ajax.php"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Content-Type": "application/x-www-form-urlencoded"
}


def fetch_history(period: str = "1year") -> list:
    """
    Fetch historical rates.
    period: 1week, 1month, 3month, 6month, 1year
    Returns list of {date, buy, sell} dicts.
    """
    params = {
        "action": "tgc_currency_history",
        "source": "USD",
        "period": period
    }

    try:
        resp = requests.get(API_URL, params=params, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        # Handle new format: {dates: [], buy: [], sell: []}
        if isinstance(data, dict) and "dates" in data and "buy" in data:
            dates = data.get("dates", [])
            buys = data.get("buy", [])
            sells = data.get("sell", [])
            records = []
            for i, date_val in enumerate(dates):
                records.append({
                    "date": date_val,
                    "buy": buys[i] if i < len(buys) else None,
                    "sell": sells[i] if i < len(sells) else None
                })
            return records
        elif isinstance(data, dict) and "data" in data:
            return data["data"]
        elif isinstance(data, list):
            return data
        else:
            print(f"Unexpected response format: {type(data)}")
            return []

    except requests.RequestException as e:
        print(f"Request error: {e}")
        return []
    except ValueError as e:
        print(f"JSON parse error: {e}")
        return []


def parse_rate(value) -> float:
    """Parse rate value."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.replace(',', '').replace(' ', '')
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def import_history(period: str = "1year"):
    """Fetch and import historical data."""
    print(f"Fetching {period} history from tygiachoden.com...")
    start_time = time.time()

    records = fetch_history(period)

    if not records:
        log_scrape("tygiachoden.com", period, "error", 0, "No data returned", time.time() - start_time)
        print("No records returned")
        return 0

    imported = 0
    for record in records:
        try:
            # Parse date - could be various formats
            date_val = record.get("date") or record.get("ngay") or record.get("time")
            if not date_val:
                continue

            # Try to parse date
            if isinstance(date_val, str):
                for fmt in ["%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y"]:
                    try:
                        dt = datetime.strptime(date_val, fmt)
                        iso_date = dt.strftime("%Y-%m-%d")
                        break
                    except ValueError:
                        continue
                else:
                    continue
            else:
                continue

            # Extract rates
            buy = parse_rate(record.get("buy") or record.get("mua"))
            sell = parse_rate(record.get("sell") or record.get("ban"))

            if buy or sell:
                upsert_rate(iso_date, {
                    "tygiachoden_buy": buy,
                    "tygiachoden_sell": sell
                })
                compute_grey_premium(iso_date)
                imported += 1

        except Exception as e:
            print(f"Error processing record {record}: {e}")
            continue

    duration = time.time() - start_time
    log_scrape("tygiachoden.com", period, "success", imported, None, duration)
    print(f"Imported {imported} records in {duration:.1f}s")
    return imported


def scrape_today():
    """Fetch latest rates (1 week to ensure today is included)."""
    print("Fetching latest rates from tygiachoden.com...")
    start_time = time.time()

    records = fetch_history("1week")

    if not records:
        today = datetime.now().strftime("%Y-%m-%d")
        log_scrape("tygiachoden.com", today, "error", 0, "No data", time.time() - start_time)
        return None

    # Get most recent record
    latest = records[-1] if records else None
    if latest:
        date_val = latest.get("date") or latest.get("ngay")
        buy = parse_rate(latest.get("buy") or latest.get("mua"))
        sell = parse_rate(latest.get("sell") or latest.get("ban"))

        if date_val:
            for fmt in ["%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y"]:
                try:
                    dt = datetime.strptime(date_val, fmt)
                    iso_date = dt.strftime("%Y-%m-%d")
                    break
                except ValueError:
                    continue
            else:
                iso_date = datetime.now().strftime("%Y-%m-%d")

            data = {"tygiachoden_buy": buy, "tygiachoden_sell": sell}
            upsert_rate(iso_date, data)
            compute_grey_premium(iso_date)

            log_scrape("tygiachoden.com", iso_date, "success", 1, None, time.time() - start_time)
            print(f"Latest rate ({iso_date}): buy={buy}, sell={sell}")
            return data

    return None


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="tygiachoden.com scraper")
    parser.add_argument("--backfill", action="store_true", help="Import 1 year history")
    parser.add_argument("--period", type=str, default="1year",
                        choices=["1week", "1month", "3month", "6month", "1year"],
                        help="History period to fetch")
    parser.add_argument("--today", action="store_true", help="Fetch latest rates")

    args = parser.parse_args()

    if args.backfill:
        import_history(args.period)
    elif args.today:
        scrape_today()
    else:
        # Default: import full year
        import_history("1year")
