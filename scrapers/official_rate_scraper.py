"""
Official USD/VND Rate Scraper
Fetches official exchange rates from free APIs.
"""

import requests
from datetime import datetime, timedelta
import time
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from database import upsert_rate, log_scrape

# Free exchange rate APIs
EXCHANGERATE_API = "https://open.er-api.com/v6/latest/USD"
FRANKFURTER_API = "https://api.frankfurter.app"

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}


def fetch_current_rate() -> float:
    """Fetch current official USD/VND rate."""
    try:
        resp = requests.get(EXCHANGERATE_API, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        if data.get("result") == "success":
            return data.get("rates", {}).get("VND")
        return None

    except requests.RequestException as e:
        print(f"ExchangeRate API error: {e}")
        return None


def fetch_historical_frankfurter(start_date: str, end_date: str) -> dict:
    """
    Fetch historical rates from Frankfurter API.
    Note: Frankfurter doesn't have VND, so this is a fallback.
    Returns dict of {date: rate}
    """
    url = f"{FRANKFURTER_API}/{start_date}..{end_date}"
    params = {"from": "USD", "to": "VND"}

    try:
        resp = requests.get(url, params=params, headers=HEADERS, timeout=60)
        if resp.status_code == 400:
            # VND not supported
            return {}
        resp.raise_for_status()
        data = resp.json()

        rates = {}
        for date_str, rate_data in data.get("rates", {}).items():
            if "VND" in rate_data:
                rates[date_str] = rate_data["VND"]
        return rates

    except requests.RequestException as e:
        print(f"Frankfurter API error: {e}")
        return {}


def scrape_today():
    """Fetch today's official rate."""
    print("Fetching official USD/VND rate...")
    start_time = time.time()

    rate = fetch_current_rate()
    today = datetime.now().strftime("%Y-%m-%d")

    if rate:
        upsert_rate(today, {"official_usd_vnd": round(rate, 2)})
        log_scrape("official", today, "success", 1, None, time.time() - start_time)
        print(f"Official USD/VND: {rate:,.0f}")
        return rate
    else:
        log_scrape("official", today, "error", 0, "No rate", time.time() - start_time)
        print("Failed to fetch official rate")
        return None


def backfill_from_tygiausd():
    """
    Note: The official rate is already captured from tygiausd.org
    in the tygiausd_sbv_central column. This function is for
    supplementary official rate data from other APIs.
    """
    print("Note: Official rates are primarily captured from tygiausd.org (sbv_central)")
    print("This API provides current rates only.")
    scrape_today()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Official USD/VND rate scraper")
    parser.add_argument("--today", action="store_true", help="Fetch current rate")

    args = parser.parse_args()

    scrape_today()
