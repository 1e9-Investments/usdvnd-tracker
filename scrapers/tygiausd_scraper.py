"""
tygiausd.org Scraper
Primary grey market source - has data back to May 2014.
URL pattern: https://tygiausd.org/TyGia?date=DD-MM-YYYY
"""

import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import time
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from database import upsert_rate, log_scrape, compute_grey_premium, get_connection

BASE_URL = "https://tygiausd.org/TyGia"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}


def parse_rate(text: str) -> float:
    """Parse rate from text, handling Vietnamese formatting.

    Handles cases like "27,930 -20" where -20 is the daily change.
    """
    if not text:
        return None

    text = text.strip()

    # Split on space to separate rate from change indicator (e.g., "27,930 -20")
    parts = text.split()
    if parts:
        rate_part = parts[0]  # Take only the first number
    else:
        rate_part = text

    # Remove non-numeric except comma and dot
    cleaned = re.sub(r'[^\d,.]', '', rate_part)
    if not cleaned:
        return None

    # Vietnamese uses comma as thousands separator
    cleaned = cleaned.replace(',', '')

    try:
        return float(cleaned)
    except ValueError:
        return None


def scrape_date(date_str: str) -> dict:
    """
    Scrape rates for a specific date.
    date_str format: DD-MM-YYYY
    Returns dict with rates or empty dict on failure.
    """
    url = f"{BASE_URL}?date={date_str}"

    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  Request error for {date_str}: {e}")
        return {}

    soup = BeautifulSoup(resp.text, 'html.parser')
    data = {}

    # Look for grey market rates (tự do / chợ đen)
    # The page structure has tables with rate information
    tables = soup.find_all('table')

    for table in tables:
        rows = table.find_all('tr')
        for row in rows:
            cells = row.find_all(['td', 'th'])
            if len(cells) >= 2:
                label = cells[0].get_text(strip=True).lower()

                # Grey market / free market rates
                if 'tự do' in label or 'chợ đen' in label or 'tự do' in label:
                    if len(cells) >= 3:
                        data['tygiausd_grey_buy'] = parse_rate(cells[1].get_text())
                        data['tygiausd_grey_sell'] = parse_rate(cells[2].get_text())
                    elif len(cells) >= 2:
                        data['tygiausd_grey_sell'] = parse_rate(cells[1].get_text())

                # SBV Central rate
                elif 'ngân hàng nhà nước' in label or 'sbv' in label or 'trung tâm' in label:
                    data['tygiausd_sbv_central'] = parse_rate(cells[1].get_text())

                # Vietcombank rates
                elif 'vietcombank' in label or 'vcb' in label:
                    if len(cells) >= 3:
                        data['tygiausd_vcb_buy'] = parse_rate(cells[1].get_text())
                        data['tygiausd_vcb_sell'] = parse_rate(cells[2].get_text())

    # Also try to find rates in divs/spans with specific classes
    # Look for any element containing rate-like numbers near keywords
    text = soup.get_text()

    # Pattern: "Tự do" or "Chợ đen" followed by numbers
    grey_pattern = r'(?:tự do|chợ đen)[:\s]*(?:mua[:\s]*)?(\d{2}[,.]?\d{3})[^\d]*(?:bán[:\s]*)?(\d{2}[,.]?\d{3})?'
    grey_match = re.search(grey_pattern, text, re.IGNORECASE)
    if grey_match and not data.get('tygiausd_grey_buy'):
        data['tygiausd_grey_buy'] = parse_rate(grey_match.group(1))
        if grey_match.group(2):
            data['tygiausd_grey_sell'] = parse_rate(grey_match.group(2))

    # SBV pattern
    sbv_pattern = r'(?:ngân hàng nhà nước|sbv|trung tâm)[:\s]*(\d{2}[,.]?\d{3})'
    sbv_match = re.search(sbv_pattern, text, re.IGNORECASE)
    if sbv_match and not data.get('tygiausd_sbv_central'):
        data['tygiausd_sbv_central'] = parse_rate(sbv_match.group(1))

    return data


def scrape_date_iso(iso_date: str) -> dict:
    """Wrapper that accepts YYYY-MM-DD format."""
    dt = datetime.strptime(iso_date, "%Y-%m-%d")
    vn_date = dt.strftime("%d-%m-%Y")
    return scrape_date(vn_date)


def backfill(start_date: str, end_date: str, delay: float = 1.5):
    """
    Backfill historical data.
    start_date, end_date: YYYY-MM-DD format
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    current = start
    total_days = (end - start).days + 1
    success_count = 0
    error_count = 0

    print(f"Starting backfill from {start_date} to {end_date} ({total_days} days)")
    start_time = time.time()

    while current <= end:
        iso_date = current.strftime("%Y-%m-%d")
        vn_date = current.strftime("%d-%m-%Y")

        day_num = (current - start).days + 1
        print(f"[{day_num}/{total_days}] Scraping {iso_date}...", end=" ")

        try:
            data = scrape_date(vn_date)

            if data:
                upsert_rate(iso_date, data)
                compute_grey_premium(iso_date)
                print(f"OK - grey: {data.get('tygiausd_grey_buy')}/{data.get('tygiausd_grey_sell')}")
                success_count += 1
            else:
                print("No data found")

        except Exception as e:
            print(f"ERROR: {e}")
            error_count += 1

        current += timedelta(days=1)
        time.sleep(delay)

    duration = time.time() - start_time
    log_scrape("tygiausd.org", f"{start_date} to {end_date}",
               "success" if error_count == 0 else "partial",
               success_count, f"{error_count} errors", duration)

    print(f"\nBackfill complete: {success_count} success, {error_count} errors in {duration:.1f}s")
    return success_count, error_count


def scrape_today():
    """Scrape today's rates."""
    today = datetime.now().strftime("%Y-%m-%d")
    vn_today = datetime.now().strftime("%d-%m-%Y")

    print(f"Scraping today ({today})...")
    start_time = time.time()

    try:
        data = scrape_date(vn_today)
        if data:
            upsert_rate(today, data)
            compute_grey_premium(today)
            log_scrape("tygiausd.org", today, "success", 1, None, time.time() - start_time)
            print(f"Success: {data}")
            return data
        else:
            log_scrape("tygiausd.org", today, "error", 0, "No data found", time.time() - start_time)
            print("No data found")
            return None
    except Exception as e:
        log_scrape("tygiausd.org", today, "error", 0, str(e), time.time() - start_time)
        print(f"Error: {e}")
        return None


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="tygiausd.org scraper")
    parser.add_argument("--backfill", action="store_true", help="Run full 5-year backfill")
    parser.add_argument("--start", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, help="End date (YYYY-MM-DD)")
    parser.add_argument("--today", action="store_true", help="Scrape today only")
    parser.add_argument("--test", type=str, help="Test scrape single date (DD-MM-YYYY)")

    args = parser.parse_args()

    if args.test:
        result = scrape_date(args.test)
        print(f"Result: {result}")
    elif args.today:
        scrape_today()
    elif args.backfill or args.start:
        end_date = args.end or datetime.now().strftime("%Y-%m-%d")
        start_date = args.start or "2021-03-24"  # 5 years ago
        backfill(start_date, end_date)
    else:
        # Default: scrape today
        scrape_today()
