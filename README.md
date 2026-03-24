# USDVND Rate Tracker

Historical and real-time USD/VND exchange rate tracking, comparing official rates with grey/black market rates.

## Data Sources

| Source | Type | Historical | Update Frequency |
|--------|------|------------|------------------|
| [tygiausd.org](https://tygiausd.org) | Grey market (gold shops) | 2014-present | Daily |
| [tygiachoden.com](https://tygiachoden.com) | Grey market | ~1 year | Daily |
| [chogia.vn](https://chogia.vn) | Grey market | 30 days rolling | Daily |
| [Binance P2P](https://p2p.binance.com) | USDT/VND P2P | Forward only | Real-time |
| [CoinGecko](https://coingecko.com) | USDT/VND (official conversion) | ~1 year | Daily |
| [ExchangeRate API](https://open.er-api.com) | Official USD/VND | Current only | Daily |

## Output Files

- `data/usdvnd_rates.db` — SQLite database with all historical rates
- `data/usdvnd_rates.csv` — CSV export of all data
- `analysis/USDVND_Analysis.xlsx` — Excel workbook with analysis and charts

## Excel Analysis

The Excel workbook includes:
- **Summary** — Latest rates, spreads, depreciation metrics
- **Time Series** — Full historical data
- **Charts** — Official vs grey market rates, spread over time
- **Monthly Summary** — Monthly average rates and spreads
- **Source Comparison** — Cross-source rate comparison

## Key Metrics

- **Grey Market Premium** — `(grey_mid - official) / official × 100`
- **YTD Depreciation** — VND depreciation since Jan 1
- **1Y Depreciation** — Trailing 12-month depreciation

## Usage

### Setup
```bash
cd ~/Code/workspace/usdvnd-tracker
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python scrapers/database.py  # Initialize DB
```

### Daily Update
```bash
source venv/bin/activate
python scrapers/daily_update.py
python analysis/generate_excel.py
```

### Historical Backfill
```bash
# Quick backfill (API sources only, ~2 min)
python scrapers/backfill.py --quick

# Full backfill including tygiausd.org (45-90 min)
python scrapers/backfill.py --full --years 5
```

### Individual Scrapers
```bash
python scrapers/tygiausd_scraper.py --today
python scrapers/binance_p2p_scraper.py
python scrapers/coingecko_scraper.py --today
```

## Automation

Daily updates run via cron at 12:00 UTC (7 PM Vietnam time):
```
0 12 * * * /home/1e9investments/Code/workspace/usdvnd-tracker/scripts/daily_cron.sh
```

## License

MIT

---
*Maintained by 1e9-Investments*
