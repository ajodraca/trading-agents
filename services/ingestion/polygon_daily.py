#!/usr/bin/env python3
"""
Fetch daily OHLCV from Polygon for a universe of tickers and write to Postgres & ClickHouse.
Also fetches corporate actions (splits & dividends) for the same universe.

Usage examples:
    # In project root where .env lives
    python services/ingestion/polygon_daily.py \
        --start 2020-01-01 --end today \
        --universe-file data/universe/sp100.csv \
        --fetch-events

Env vars expected (see .env):
    POLYGON_API_KEY
    POSTGRES_URI
    CLICKHOUSE_URL

Notes:
  - Share class tickers use dot notation on Polygon (e.g., BRK.B).
  - The script is idempotent: it upserts into Postgres; ClickHouse inserts are de-duped by full reload or by pre-delete window.
"""
from __future__ import annotations

import os
import sys
import time
import math
import random
import argparse
import datetime as dt
from typing import List, Dict, Any, Iterable

import requests
import pandas as pd
from loguru import logger
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from clickhouse_connect import get_client as get_ch_client

# ----------------------------
# Config & helpers
# ----------------------------
load_dotenv()
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
POSTGRES_URI = os.getenv("POSTGRES_URI")
CLICKHOUSE_URL = os.getenv("CLICKHOUSE_URL", "localhost:8123")

if not POLYGON_API_KEY:
    logger.error("POLYGON_API_KEY is not set in your .env; please add it.")
    sys.exit(1)

SESSION = requests.Session()
SESSION.headers.update({"Authorization": f"Bearer {POLYGON_API_KEY}"})  # Polygon auth

ISO = "%Y-%m-%d"
AGGS_URL_TMPL = "https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start}/{end}"
SPLITS_URL = "https://api.polygon.io/v3/reference/splits"
DIVS_URL = "https://api.polygon.io/v3/reference/dividends"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Ingest daily OHLCV + corp actions from Polygon")
    p.add_argument("--start", required=True, help="YYYY-MM-DD or 'yesterday' or 'today'")
    p.add_argument("--end", default="today", help="YYYY-MM-DD or 'today'")
    p.add_argument("--universe-file", required=True, help="CSV with header 'ticker'")
    p.add_argument("--sleep", type=float, default=0.25, help="Seconds between API calls")
    p.add_argument("--fetch-events", action="store_true", help="Also fetch splits & dividends")
    p.add_argument("--delete-existing-window", type=int, default=0,
                   help="If >0, delete existing rows in Postgres for last N days before insert")
    return p.parse_args()


def norm_date(s: str) -> str:
    s = s.strip().lower()
    today = dt.date.today()
    if s == "today":
        return today.strftime(ISO)
    if s == "yesterday":
        return (today - dt.timedelta(days=1)).strftime(ISO)
    # Assume already in ISO
    return s


def load_universe(csv_path: str) -> List[str]:
    df = pd.read_csv(csv_path)
    if "ticker" not in df.columns:
        raise ValueError("Universe CSV must have a 'ticker' column")
    tickers = (
        df["ticker"].astype(str).str.strip().str.upper().tolist()
    )
    # Drop obvious blanks
    return [t for t in tickers if t]


# ----------------------------
# Polygon fetchers
# ----------------------------

def http_get(url: str, params: Dict[str, Any] | None = None, retries: int = 7) -> Dict[str, Any]:
    base_wait = 12  # Base wait time in seconds
    
    for attempt in range(1, retries + 1):
        try:
            r = SESSION.get(url, params=params, timeout=30)
            
            if r.status_code == 429:
                # Rate limited - use exponential backoff with jitter
                max_wait = min(120, base_wait * (2 ** (attempt - 1)))  # max 120 seconds
                jitter = random.uniform(0, min(5, attempt))  # Add some randomness
                wait = max_wait + jitter
                
                logger.warning(f"429 rate limit on {url}")
                logger.warning(f"Attempt {attempt}/{retries}, backing off {wait:.1f}s ...")
                
                time.sleep(wait)
                continue
                
            r.raise_for_status()
            
            # Success - add a small delay to prevent rate limits
            time.sleep(0.2)  # 200ms between successful requests
            
            return r.json()
            
        except requests.exceptions.ReadTimeout:
            wait = min(60, base_wait * attempt)
            logger.warning(f"Timeout on {url}")
            logger.warning(f"Attempt {attempt}/{retries}, waiting {wait}s ...")
            time.sleep(wait)
            
        except Exception as e:
            wait = min(60, base_wait * attempt)
            logger.warning(f"GET failed ({type(e).__name__}) on {url}: {str(e)}")
            logger.warning(f"Attempt {attempt}/{retries}, waiting {wait}s ...")
            time.sleep(wait)
            
    raise RuntimeError(f"Failed GET after {retries} attempts: {url}")


def fetch_daily_aggregates(ticker: str, start: str, end: str) -> pd.DataFrame:
    url = AGGS_URL_TMPL.format(ticker=ticker, start=start, end=end)
    params = {
        "adjusted": "true",  # explicit; Polygon defaults to adjusted
        "sort": "asc",
        "limit": 50000,
    }
    data = http_get(url, params)
    results = data.get("results", []) or []
    if not results:
        return pd.DataFrame(columns=[
            "ticker", "trade_date", "open", "high", "low", "close", "volume", "vwap", "adjusted_close"
        ])
    rows = []
    for row in results:
        # Polygon fields: o,h,l,c,v,vw,t
        ts_ms = int(row.get("t"))
        trade_date = dt.datetime.fromtimestamp(ts_ms / 1000, dt.UTC).date()
        rows.append({
            "ticker": ticker,
            "trade_date": trade_date,
            "open": row.get("o"),
            "high": row.get("h"),
            "low": row.get("l"),
            "close": row.get("c"),
            "volume": int(row.get("v")) if row.get("v") is not None else None,
            "vwap": row.get("vw"),
            "adjusted_close": row.get("c"),  # adjusted=true so c is adjusted
        })
    return pd.DataFrame.from_records(rows)


def fetch_splits(ticker: str, start: str, end: str) -> pd.DataFrame:
    params = {
        "ticker": ticker,
        "execution_date.gte": start,
        "execution_date.lte": end,
        "order": "asc",
        "limit": 1000,
    }
    data = http_get(SPLITS_URL, params)
    results = data.get("results", []) or []
    if not results:
        return pd.DataFrame(columns=[
            "ticker", "ca_type", "ex_date", "record_date", "payable_date", "declared_date",
            "split_from", "split_to", "amount", "frequency"
        ])
    rows = []
    for r in results:
        try:
            # Convert dates at the source
            ex_date = pd.to_datetime(r.get("execution_date")).date() if r.get("execution_date") else None
            
            rows.append({
                "ticker": r.get("ticker"),
                "ca_type": "split",
                "ex_date": ex_date,
                "record_date": None,
                "payable_date": None,
                "declared_date": None,
                "split_from": r.get("split_from"),
                "split_to": r.get("split_to"),
                "amount": None,
                "frequency": None,
            })
        except Exception as e:
            logger.error(f"Error processing split data for {ticker}: {e}")
            logger.error(f"Raw data: {r}")
            continue
            
    return pd.DataFrame.from_records(rows)


def fetch_dividends(ticker: str, start: str, end: str) -> pd.DataFrame:
    params = {
        "ticker": ticker,
        "ex_dividend_date.gte": start,
        "ex_dividend_date.lte": end,
        "order": "asc",
        "limit": 1000,
    }
    data = http_get(DIVS_URL, params)
    results = data.get("results", []) or []
    if not results:
        return pd.DataFrame(columns=[
            "ticker", "ca_type", "ex_date", "record_date", "payable_date", "declared_date",
            "split_from", "split_to", "amount", "frequency"
        ])
    rows = []
    for r in results:
        try:
            # Convert dates at the source
            ex_date = pd.to_datetime(r.get("ex_dividend_date")).date() if r.get("ex_dividend_date") else None
            record_date = pd.to_datetime(r.get("record_date")).date() if r.get("record_date") else None
            payable_date = pd.to_datetime(r.get("pay_date")).date() if r.get("pay_date") else None
            declared_date = pd.to_datetime(r.get("declaration_date")).date() if r.get("declaration_date") else None
            
            rows.append({
                "ticker": r.get("ticker"),
                "ca_type": "dividend",
                "ex_date": ex_date,
                "record_date": record_date,
                "payable_date": payable_date,
                "declared_date": declared_date,
                "split_from": None,
                "split_to": None,
                "amount": float(r.get("cash_amount")) if r.get("cash_amount") is not None else None,
                "frequency": str(r.get("frequency")) if r.get("frequency") is not None else None,
            })
        except Exception as e:
            logger.error(f"Error processing dividend data for {ticker}: {e}")
            logger.error(f"Raw data: {r}")
            continue
            
    return pd.DataFrame.from_records(rows)


# ----------------------------
# Storage: Postgres & ClickHouse
# ----------------------------

def init_postgres(engine: Engine):
    with engine.begin() as conn:
        conn.execute(
            text("""
                CREATE SCHEMA IF NOT EXISTS core;
                CREATE TABLE IF NOT EXISTS core.prices_daily (
                    ticker           TEXT        NOT NULL,
                    trade_date       DATE        NOT NULL,
                    open             DOUBLE PRECISION,
                    high             DOUBLE PRECISION,
                    low              DOUBLE PRECISION,
                    close            DOUBLE PRECISION,
                    volume           BIGINT,
                    vwap             DOUBLE PRECISION,
                    adjusted_close   DOUBLE PRECISION,
                    PRIMARY KEY (ticker, trade_date)
                );
                CREATE TABLE IF NOT EXISTS core.corporate_actions (
                    ticker           TEXT        NOT NULL,
                    ca_type          TEXT        NOT NULL,
                    ex_date          DATE,
                    record_date      DATE,
                    payable_date     DATE,
                    declared_date    DATE,
                    split_from       NUMERIC(18,8),
                    split_to         NUMERIC(18,8),
                    amount           NUMERIC(18,8),
                    frequency        TEXT,
                    source           TEXT DEFAULT 'polygon',
                    PRIMARY KEY (ticker, ca_type, ex_date)
                );
            """)
        )


def init_clickhouse():
    # Parse host and port from URL
    url = CLICKHOUSE_URL.replace('http://', '').split(':')[0]  # get just the host
    port = int(CLICKHOUSE_URL.split(':')[-1])  # get the port
    client = get_ch_client(host=url, port=port)
    
    # Create database
    client.command("CREATE DATABASE IF NOT EXISTS core")
    
    # Create prices table
    client.command("""
        CREATE TABLE IF NOT EXISTS core.prices_daily (
            ticker           String,
            trade_date       Date,
            open            Float64,
            high            Float64,
            low             Float64,
            close           Float64,
            volume          Int64,
            vwap            Float64,
            adjusted_close  Float64
        ) ENGINE = ReplacingMergeTree()
        ORDER BY (ticker, trade_date)
    """)
    
    # Create corporate actions table with non-nullable key columns
    client.command("""
        CREATE TABLE IF NOT EXISTS core.corporate_actions (
            ticker          String,
            ca_type         String,
            ex_date         Date,  -- Changed from Nullable(Date) since it's part of the key
            record_date     Nullable(Date),
            payable_date    Nullable(Date),
            declared_date   Nullable(Date),
            split_from      Nullable(Float64),
            split_to        Nullable(Float64),
            amount         Nullable(Float64),
            frequency      Nullable(String),
            source         String DEFAULT 'polygon'
        ) ENGINE = ReplacingMergeTree()
        ORDER BY (ticker, ca_type, ex_date)
    """)


def store_prices(prices_df: pd.DataFrame, pg_engine: Engine, delete_window: int = 0):
    if prices_df.empty:
        return
        
    # Store in Postgres with upsert
    with pg_engine.begin() as conn:
        if delete_window > 0:
            # Delete recent data in case of corrections
            conn.execute(text(
                "DELETE FROM core.prices_daily WHERE trade_date >= current_date - :days",
                {"days": delete_window}
            ))
        
        # Create temporary table
        temp_table = "temp_prices_daily"
        prices_df.to_sql(
            temp_table,
            conn,
            schema="core",
            if_exists="replace",
            index=False,
            method="multi",
        )
        
        # Perform upsert using the temporary table
        conn.execute(text(f"""
            INSERT INTO core.prices_daily (
                ticker, trade_date, open, high, low, close, 
                volume, vwap, adjusted_close
            )
            SELECT 
                ticker, trade_date, open, high, low, close,
                volume, vwap, adjusted_close
            FROM core.{temp_table}
            ON CONFLICT (ticker, trade_date) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                vwap = EXCLUDED.vwap,
                adjusted_close = EXCLUDED.adjusted_close
        """))
        
        # Drop temporary table
        conn.execute(text(f"DROP TABLE core.{temp_table}"))
    
    # Store in ClickHouse
    url = CLICKHOUSE_URL.replace('http://', '').split(':')[0]  # get just the host
    port = int(CLICKHOUSE_URL.split(':')[-1])  # get the port
    client = get_ch_client(host=url, port=port)
    if delete_window > 0:
        client.command(
            "ALTER TABLE core.prices_daily DELETE WHERE trade_date >= subtractDays(today(), :days)",
            parameters={"days": delete_window}
        )
    client.insert_df("core.prices_daily", prices_df)


def store_events(events_df: pd.DataFrame, pg_engine: Engine):
    if events_df.empty:
        return

    try:
        # Make a copy to avoid modifying the original DataFrame
        df = events_df.copy()
        
        # Convert date strings to datetime.date objects and handle NaT values
        for date_col in ['ex_date', 'record_date', 'payable_date', 'declared_date']:
            # First convert to datetime
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            # Then convert to date, but only for non-NaT values
            mask = df[date_col].notna()
            if mask.any():
                df.loc[mask, date_col] = df.loc[mask, date_col].dt.date
                
        # Convert numeric columns and handle NaN values
        for col in ['split_from', 'split_to', 'amount']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Remove duplicate rows based on the primary key columns (ticker, ca_type, ex_date)
        # Keep the row with the latest record_date when there are duplicates
        df = df.sort_values('record_date', na_position='first').drop_duplicates(
            subset=['ticker', 'ca_type', 'ex_date'], 
            keep='last'
        )
        
        # Store in Postgres with upsert
        with pg_engine.begin() as conn:
            # Create temporary table with explicit types
            conn.execute(text("""
                CREATE TEMP TABLE temp_corporate_actions (
                    ticker TEXT,
                    ca_type TEXT,
                    ex_date DATE,
                    record_date DATE,
                    payable_date DATE,
                    declared_date DATE,
                    split_from NUMERIC(18,8),
                    split_to NUMERIC(18,8),
                    amount NUMERIC(18,8),
                    frequency TEXT
                )
            """))
            
            # Insert data using parameterized query
            if not df.empty:
                for _, row in df.iterrows():
                    # Convert row to dict and handle NaT/NaN values
                    values = {}
                    for key, val in row.items():
                        if pd.isna(val):
                            values[key] = None
                        elif isinstance(val, pd.Timestamp):
                            values[key] = val.date()
                        else:
                            values[key] = val
                    
                    conn.execute(
                        text("""
                            INSERT INTO temp_corporate_actions (
                                ticker, ca_type, ex_date, record_date, payable_date,
                                declared_date, split_from, split_to, amount, frequency
                            ) VALUES (
                                :ticker, :ca_type, :ex_date, :record_date, :payable_date,
                                :declared_date, :split_from, :split_to, :amount, :frequency
                            )
                        """),
                        values
                    )
            
            # Perform upsert from temp table
            conn.execute(text("""
                INSERT INTO core.corporate_actions (
                    ticker, ca_type, ex_date, record_date, payable_date,
                    declared_date, split_from, split_to, amount, frequency
                )
                SELECT 
                    ticker, ca_type, ex_date, record_date, payable_date,
                    declared_date, split_from, split_to, amount, frequency
                FROM temp_corporate_actions
                ON CONFLICT (ticker, ca_type, ex_date) DO UPDATE SET
                    record_date = EXCLUDED.record_date,
                    payable_date = EXCLUDED.payable_date,
                    declared_date = EXCLUDED.declared_date,
                    split_from = EXCLUDED.split_from,
                    split_to = EXCLUDED.split_to,
                    amount = EXCLUDED.amount,
                    frequency = EXCLUDED.frequency
            """))
            
            # Drop temporary table
            conn.execute(text("DROP TABLE temp_corporate_actions"))
        
        # Store in ClickHouse
        url = CLICKHOUSE_URL.replace('http://', '').split(':')[0]  # get just the host
        port = int(CLICKHOUSE_URL.split(':')[-1])  # get the port
        client = get_ch_client(host=url, port=port)
        
        # Convert NaT/NaN values to None for ClickHouse
        df_ch = df.copy()
        for col in df_ch.columns:
            df_ch[col] = df_ch[col].where(pd.notna(df_ch[col]), None)
            
        client.insert_df("core.corporate_actions", df_ch)
        
    except Exception as e:
        logger.error(f"Error storing events: {str(e)}")
        logger.error("Data sample:")
        logger.error(df.head().to_string())
        logger.error("Data types:")
        logger.error(df.dtypes.to_string())
        raise


def main():
    args = parse_args()
    start_date = norm_date(args.start)
    end_date = norm_date(args.end)
    tickers = load_universe(args.universe_file)
    logger.info(f"Processing {len(tickers)} tickers from {start_date} to {end_date}")
    
    # Initialize databases
    pg_engine = create_engine(POSTGRES_URI)
    init_postgres(pg_engine)
    init_clickhouse()
    
    # Process each ticker
    for ticker in tickers:
        logger.info(f"Processing {ticker}")
        try:
            # Fetch and store prices
            prices_df = fetch_daily_aggregates(ticker, start_date, end_date)
            if not prices_df.empty:
                logger.info(f"Got {len(prices_df)} price rows for {ticker}")
                store_prices(prices_df, pg_engine, args.delete_existing_window)
            
            # Fetch and store corporate actions if requested
            if args.fetch_events:
                splits_df = fetch_splits(ticker, start_date, end_date)
                divs_df = fetch_dividends(ticker, start_date, end_date)
                events_df = pd.concat([splits_df, divs_df], ignore_index=True)
                if not events_df.empty:
                    logger.info(f"Got {len(events_df)} events for {ticker}")
                    store_events(events_df, pg_engine)
            
            # Rate limit compliance
            time.sleep(args.sleep)
            
        except Exception as e:
            logger.error(f"Error processing {ticker}: {e}")
            continue


if __name__ == "__main__":
    main()