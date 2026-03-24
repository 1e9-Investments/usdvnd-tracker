"""
Binance P2P Scraper
USDT/VND P2P rates - true "crypto grey market" rates.
No historical data available - forward collection only.
"""

import requests
from datetime import datetime
import statistics
import time
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from database import upsert_rate, log_scrape, compute_grey_premium

P2P_URL = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Content-Type": "application/json"
}


def get_p2p_rates(trade_type: str = "BUY", rows: int = 10) -> list:
    """
    Fetch P2P advertisement rates.
    trade_type: BUY (user buys USDT) or SELL (user sells USDT)
    Returns list of prices from top merchant ads.
    """
    payload = {
        "asset": "USDT",
        "fiat": "VND",
        "tradeType": trade_type,
        "page": 1,
        "rows": rows,
        "publisherType": "merchant",  # Only verified merchants
        "payTypes": []  # All payment methods
    }

    try:
        resp = requests.post(P2P_URL, json=payload, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        ads = data.get("data", [])
        prices = []

        for ad in ads:
            adv = ad.get("adv", {})
            price = adv.get("price")
            if price:
                prices.append(float(price))

        return prices

    except requests.RequestException as e:
        print(f"Request error for {trade_type}: {e}")
        return []


def scrape_current() -> dict:
    """
    Scrape current P2P rates.
    Returns dict with buy/sell median prices.
    """
    print("Fetching Binance P2P USDT/VND rates...")
    start_time = time.time()

    # BUY = user buying USDT (paying VND) - this is the "sell" rate for VND
    # SELL = user selling USDT (receiving VND) - this is the "buy" rate for VND

    buy_prices = get_p2p_rates("BUY", 10)
    time.sleep(0.5)  # Small delay between requests
    sell_prices = get_p2p_rates("SELL", 10)

    result = {}
    today = datetime.now().strftime("%Y-%m-%d")

    if buy_prices:
        # When user BUYS USDT, they pay this much VND per USDT
        # This is the higher rate (user pays more)
        result["binance_p2p_sell"] = round(statistics.median(buy_prices), 0)
        print(f"  BUY USDT (sell VND): {result['binance_p2p_sell']:,.0f} VND (median of {len(buy_prices)} ads)")

    if sell_prices:
        # When user SELLS USDT, they receive this much VND per USDT
        # This is the lower rate (user receives less)
        result["binance_p2p_buy"] = round(statistics.median(sell_prices), 0)
        print(f"  SELL USDT (buy VND): {result['binance_p2p_buy']:,.0f} VND (median of {len(sell_prices)} ads)")

    if result:
        upsert_rate(today, result)
        compute_grey_premium(today)

        spread = None
        if "binance_p2p_buy" in result and "binance_p2p_sell" in result:
            spread = result["binance_p2p_sell"] - result["binance_p2p_buy"]
            spread_pct = spread / result["binance_p2p_buy"] * 100
            print(f"  Spread: {spread:,.0f} VND ({spread_pct:.2f}%)")

        log_scrape("binance_p2p", today, "success", 1, None, time.time() - start_time)
    else:
        log_scrape("binance_p2p", today, "error", 0, "No ads found", time.time() - start_time)

    return result


def get_detailed_ads(trade_type: str = "BUY", rows: int = 20) -> list:
    """Get detailed ad information for analysis."""
    payload = {
        "asset": "USDT",
        "fiat": "VND",
        "tradeType": trade_type,
        "page": 1,
        "rows": rows,
        "publisherType": "merchant",
        "payTypes": []
    }

    try:
        resp = requests.post(P2P_URL, json=payload, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        ads = data.get("data", [])
        detailed = []

        for ad in ads:
            adv = ad.get("adv", {})
            advertiser = ad.get("advertiser", {})

            detailed.append({
                "price": float(adv.get("price", 0)),
                "min_amount": float(adv.get("minSingleTransAmount", 0)),
                "max_amount": float(adv.get("maxSingleTransAmount", 0)),
                "available": float(adv.get("surplusAmount", 0)),
                "merchant_name": advertiser.get("nickName"),
                "completion_rate": float(advertiser.get("monthFinishRate", 0)) * 100,
                "order_count": advertiser.get("monthOrderCount", 0)
            })

        return detailed

    except Exception as e:
        print(f"Error: {e}")
        return []


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Binance P2P USDT/VND scraper")
    parser.add_argument("--detailed", action="store_true", help="Show detailed ad info")

    args = parser.parse_args()

    if args.detailed:
        print("\n=== BUY USDT Ads (paying VND) ===")
        buy_ads = get_detailed_ads("BUY", 10)
        for ad in buy_ads:
            print(f"  {ad['price']:>10,.0f} VND | {ad['merchant_name'][:15]:<15} | "
                  f"{ad['completion_rate']:.1f}% | {ad['order_count']} orders")

        print("\n=== SELL USDT Ads (receiving VND) ===")
        sell_ads = get_detailed_ads("SELL", 10)
        for ad in sell_ads:
            print(f"  {ad['price']:>10,.0f} VND | {ad['merchant_name'][:15]:<15} | "
                  f"{ad['completion_rate']:.1f}% | {ad['order_count']} orders")
    else:
        scrape_current()
