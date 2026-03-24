"""
USDVND Rate Tracker - Database Module
SQLite database schema and utilities for storing rates from multiple sources.
"""

import sqlite3
from datetime import datetime, date
from pathlib import Path
from typing import Optional, Dict, Any

DB_PATH = Path(__file__).parent.parent / "data" / "usdvnd_rates.db"


def get_connection() -> sqlite3.Connection:
    """Get database connection with row factory."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Initialize database with schema."""
    conn = get_connection()
    cursor = conn.cursor()

    # Main daily rates table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_rates (
            date TEXT PRIMARY KEY,

            -- tygiausd.org (primary grey market source, back to 2014)
            tygiausd_grey_buy REAL,
            tygiausd_grey_sell REAL,
            tygiausd_sbv_central REAL,
            tygiausd_vcb_buy REAL,
            tygiausd_vcb_sell REAL,

            -- tygiachoden.com (grey market, ~1 year history)
            tygiachoden_buy REAL,
            tygiachoden_sell REAL,

            -- chogia.vn (grey market, 30-day rolling)
            chogia_buy REAL,
            chogia_sell REAL,

            -- vietnambiz.vn (article-extracted, validation)
            vietnambiz_buy REAL,
            vietnambiz_sell REAL,

            -- Binance P2P USDT/VND (crypto grey market)
            binance_p2p_buy REAL,
            binance_p2p_sell REAL,

            -- CoinGecko USDT/VND (official rate baseline)
            coingecko_usdt_vnd REAL,

            -- Official rates (from other APIs)
            official_usd_vnd REAL,

            -- Computed metrics
            grey_premium_pct REAL,  -- (grey_mid - official) / official * 100

            -- Metadata
            updated_at TEXT,
            source_notes TEXT
        )
    """)

    # Scrape log table for tracking
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS scrape_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            scrape_date TEXT NOT NULL,
            status TEXT NOT NULL,  -- success, error, partial
            records_count INTEGER,
            error_message TEXT,
            duration_sec REAL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Index for faster date lookups
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_daily_rates_date
        ON daily_rates(date)
    """)

    conn.commit()
    conn.close()
    print(f"Database initialized at {DB_PATH}")


def upsert_rate(date_str: str, data: Dict[str, Any]):
    """Insert or update a rate record."""
    conn = get_connection()
    cursor = conn.cursor()

    # Get existing record
    cursor.execute("SELECT * FROM daily_rates WHERE date = ?", (date_str,))
    existing = cursor.fetchone()

    if existing:
        # Update only non-null values
        updates = []
        values = []
        for key, value in data.items():
            if value is not None:
                updates.append(f"{key} = ?")
                values.append(value)

        if updates:
            updates.append("updated_at = ?")
            values.append(datetime.now().isoformat())
            values.append(date_str)

            sql = f"UPDATE daily_rates SET {', '.join(updates)} WHERE date = ?"
            cursor.execute(sql, values)
    else:
        # Insert new record
        data['date'] = date_str
        data['updated_at'] = datetime.now().isoformat()

        columns = ', '.join(data.keys())
        placeholders = ', '.join(['?' for _ in data])
        sql = f"INSERT INTO daily_rates ({columns}) VALUES ({placeholders})"
        cursor.execute(sql, list(data.values()))

    conn.commit()
    conn.close()


def compute_grey_premium(date_str: str):
    """Compute grey market premium for a date."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT tygiausd_grey_buy, tygiausd_grey_sell,
               tygiausd_sbv_central, official_usd_vnd
        FROM daily_rates WHERE date = ?
    """, (date_str,))

    row = cursor.fetchone()
    if row:
        grey_buy = row['tygiausd_grey_buy']
        grey_sell = row['tygiausd_grey_sell']
        official = row['tygiausd_sbv_central'] or row['official_usd_vnd']

        if grey_buy and grey_sell and official:
            grey_mid = (grey_buy + grey_sell) / 2
            premium = (grey_mid - official) / official * 100

            cursor.execute("""
                UPDATE daily_rates SET grey_premium_pct = ? WHERE date = ?
            """, (round(premium, 4), date_str))
            conn.commit()

    conn.close()


def log_scrape(source: str, scrape_date: str, status: str,
               records_count: int = 0, error_message: str = None,
               duration_sec: float = None):
    """Log a scrape operation."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO scrape_log (source, scrape_date, status, records_count,
                                error_message, duration_sec)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (source, scrape_date, status, records_count, error_message, duration_sec))

    conn.commit()
    conn.close()


def get_date_range() -> tuple:
    """Get the date range of existing data."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT MIN(date), MAX(date), COUNT(*) FROM daily_rates")
    row = cursor.fetchone()
    conn.close()

    return row[0], row[1], row[2]


def export_to_csv(output_path: str = None):
    """Export all data to CSV."""
    import csv

    if output_path is None:
        output_path = DB_PATH.parent / "usdvnd_rates.csv"

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM daily_rates ORDER BY date")
    rows = cursor.fetchall()

    if rows:
        with open(output_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([desc[0] for desc in cursor.description])
            writer.writerows(rows)

        print(f"Exported {len(rows)} rows to {output_path}")

    conn.close()


if __name__ == "__main__":
    init_db()
    print("Database ready.")
