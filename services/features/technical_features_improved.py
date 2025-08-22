#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
import argparse
import datetime as dt
from typing import List, Iterator
import numpy as np
import pandas as pd
from loguru import logger
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

load_dotenv()
PG_URI = os.getenv("POSTGRES_URI")

ISO = "%Y-%m-%d"
BATCH_SIZE = 1000  # Number of days to process at once

def rsi(series: pd.Series, window: int = 14) -> pd.Series:
    """Compute Relative Strength Index.
    
    Args:
        series: Price series
        window: RSI window length
    
    Returns:
        RSI values as a series
    """
    delta = series.diff()
    up = np.clip(delta, 0, None)
    down = -np.clip(delta, None, 0)
    roll_up = up.ewm(alpha=1/window, adjust=False).mean()
    roll_down = down.ewm(alpha=1/window, adjust=False).mean()
    rs = roll_up / roll_down.replace(0, np.nan)
    return 100 - (100 / (1 + rs))

def validate_dates(start: dt.date, end: dt.date) -> None:
    """Validate date ranges.
    
    Args:
        start: Start date
        end: End date
    
    Raises:
        ValueError: If dates are invalid
    """
    if start > end:
        raise ValueError(f"Start date {start} is after end date {end}")
    if end > dt.date.today():
        raise ValueError(f"End date {end} is in the future")

def load_universe(universe_file: str) -> List[str]:
    """Load and validate universe of tickers.
    
    Args:
        universe_file: Path to CSV file with ticker column
    
    Returns:
        List of tickers
    
    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If file format is invalid
    """
    if not os.path.exists(universe_file):
        raise FileNotFoundError(f"Universe file not found: {universe_file}")
    
    try:
        tickers = pd.read_csv(universe_file)["ticker"].astype(str).str.upper().tolist()
    except Exception as e:
        raise ValueError(f"Invalid universe file format: {e}")
    
    if not tickers:
        raise ValueError("Empty universe file")
    
    return tickers

def get_date_batches(start: dt.date, end: dt.date) -> Iterator[tuple[dt.date, dt.date]]:
    """Generate date range batches to process data in chunks.
    
    Args:
        start: Overall start date
        end: Overall end date
    
    Yields:
        Tuples of (batch_start, batch_end) dates
    """
    batch_start = start
    while batch_start <= end:
        batch_end = min(
            batch_start + dt.timedelta(days=BATCH_SIZE),
            end
        )
        yield batch_start, batch_end
        batch_start = batch_end + dt.timedelta(days=1)

def compute_features(df: pd.DataFrame) -> pd.DataFrame:
    """Compute technical features for price data.
    
    Args:
        df: DataFrame with ticker, trade_date, close, volume columns
    
    Returns:
        DataFrame with computed features
    """
    if df.empty:
        return pd.DataFrame()  # Return empty frame with correct columns
        
    df = df.sort_values(["ticker", "trade_date"]).copy()
    df["ret_1d"] = df.groupby("ticker")["close"].pct_change()
    
    # group computations
    feats = []
    total = len(df["ticker"].unique())
    for i, (tkr, g) in enumerate(df.groupby("ticker", sort=False), 1):
        g = g.copy()
        g["sma_20"] = g["close"].rolling(20, min_periods=5).mean()
        g["sma_50"] = g["close"].rolling(50, min_periods=10).mean()
        g["sma_200"] = g["close"].rolling(200, min_periods=20).mean()
        g["rsi_14"] = rsi(g["close"], 14)
        g["ret_21d"] = g["close"].pct_change(21)
        g["ret_63d"] = g["close"].pct_change(63)
        g["vol_20d"] = g["ret_1d"].rolling(20, min_periods=10).std()
        g["roll_max_252"] = g["close"].rolling(252, min_periods=50).max()
        g["breakout_252"] = (g["close"] >= g["roll_max_252"]) & g["roll_max_252"].notna()
        feats.append(g[[
            "ticker","trade_date","close","volume","rsi_14","sma_20","sma_50","sma_200",
            "ret_21d","ret_63d","vol_20d","breakout_252"
        ]])
        if i % 10 == 0:  # Log progress every 10 tickers
            logger.info(f"Processed {i}/{total} tickers")
            
    out = pd.concat(feats).dropna(subset=["trade_date"])
    return out

def ensure_schema(eng) -> None:
    """Ensure database schema and tables exist.
    
    Args:
        eng: SQLAlchemy engine
    """
    with eng.begin() as con:
        # Create schema
        con.execute(text("CREATE SCHEMA IF NOT EXISTS core;"))
        
        # Create features table
        con.execute(text("""
            CREATE TABLE IF NOT EXISTS core.features_technical_daily (
              ticker TEXT NOT NULL,
              trade_date DATE NOT NULL,
              close DOUBLE PRECISION,
              volume BIGINT,
              rsi_14 DOUBLE PRECISION,
              sma_20 DOUBLE PRECISION,
              sma_50 DOUBLE PRECISION,
              sma_200 DOUBLE PRECISION,
              ret_21d DOUBLE PRECISION,
              ret_63d DOUBLE PRECISION,
              vol_20d DOUBLE PRECISION,
              breakout_252 BOOLEAN,
              PRIMARY KEY (ticker, trade_date)
            );
        """))

def store_features(feats: pd.DataFrame, eng, delete_window: int = 0, end_date: dt.date = None) -> None:
    """Store computed features in database.
    
    Args:
        feats: DataFrame with features to store
        eng: SQLAlchemy engine
        delete_window: Days of existing data to delete before insert
        end_date: End date of current processing window
    """
    if feats.empty:
        logger.warning("No features to store")
        return
        
    try:
        # Optional: clean a recent window
        if delete_window > 0 and end_date:
            cutoff = end_date - dt.timedelta(days=delete_window)
            with eng.begin() as con:
                con.execute(
                    text("DELETE FROM core.features_technical_daily WHERE trade_date >= :cutoff"),
                    {"cutoff": cutoff}
                )
                logger.info(f"Deleted existing features from {cutoff}")

        # Use temp table + merge for efficient upsert
        with eng.begin() as con:
            con.execute(text("CREATE TEMP TABLE tmp_ftd AS SELECT * FROM core.features_technical_daily WITH NO DATA;"))
            feats.to_sql("tmp_ftd", con.connection, if_exists="append", index=False)
            
            # Merge from temp to main table
            rows = con.execute(text("""
                INSERT INTO core.features_technical_daily AS t (
                  ticker, trade_date, close, volume, rsi_14, sma_20, sma_50, sma_200,
                  ret_21d, ret_63d, vol_20d, breakout_252
                )
                SELECT ticker, trade_date, close, volume, rsi_14, sma_20, sma_50, sma_200,
                       ret_21d, ret_63d, vol_20d, breakout_252
                FROM tmp_ftd
                ON CONFLICT (ticker, trade_date) DO UPDATE
                SET close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    rsi_14 = EXCLUDED.rsi_14,
                    sma_20 = EXCLUDED.sma_20,
                    sma_50 = EXCLUDED.sma_50,
                    sma_200 = EXCLUDED.sma_200,
                    ret_21d = EXCLUDED.ret_21d,
                    ret_63d = EXCLUDED.ret_63d,
                    vol_20d = EXCLUDED.vol_20d,
                    breakout_252 = EXCLUDED.breakout_252;
            """))
            
        logger.success(f"Wrote {len(feats)} feature rows")
        
    except SQLAlchemyError as e:
        logger.error(f"Database error: {e}")
        raise

def main():
    ap = argparse.ArgumentParser(description="Compute technical features from price data")
    ap.add_argument("--start", required=True, help="YYYY-MM-DD (earliest date to compute)")
    ap.add_argument("--end", default="today", help="YYYY-MM-DD or 'today'")
    ap.add_argument("--universe-file", required=True, help="CSV with header 'ticker'")
    ap.add_argument("--delete-existing-window", type=int, default=0,
                    help="If >0, delete existing features for last N days before insert")
    args = ap.parse_args()

    # Validate environment
    if not PG_URI:
        raise SystemExit("POSTGRES_URI not set in .env")

    try:
        # Parse and validate dates
        start = dt.date.fromisoformat(args.start)
        end = dt.date.today() if args.end == "today" else dt.date.fromisoformat(args.end)
        validate_dates(start, end)
        
        # Load and validate universe
        tickers = load_universe(args.universe_file)
        logger.info(f"Processing {len(tickers)} tickers from {start} to {end}")
        
        # Initialize database connection
        eng = create_engine(PG_URI, future=True)
        ensure_schema(eng)
        
        # Process in batches
        total_rows = 0
        for batch_start, batch_end in get_date_batches(start, end):
            logger.info(f"Processing batch {batch_start} to {batch_end}")
            
            # Pull prices for this batch
            with eng.connect() as con:
                df = pd.read_sql(
                    text("""
                        SELECT ticker, trade_date, close, volume
                        FROM core.prices_daily
                        WHERE ticker = ANY(:tickers) 
                        AND trade_date BETWEEN :start AND :end
                    """),
                    con,
                    params={
                        "tickers": tickers,
                        "start": batch_start,
                        "end": batch_end
                    }
                )
            
            if df.empty:
                logger.warning(f"No price data for batch {batch_start} to {batch_end}")
                continue
                
            # Compute and store features
            feats = compute_features(df)
            if not feats.empty:
                store_features(
                    feats,
                    eng,
                    delete_window=args.delete_existing_window,
                    end_date=batch_end
                )
                total_rows += len(feats)
        
        logger.success(f"Complete! Processed {total_rows} total rows")
        
    except Exception as e:
        logger.error(f"Error: {e}")
        raise

if __name__ == "__main__":
    main()
