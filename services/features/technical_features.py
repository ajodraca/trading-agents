#!/usr/bin/env python3
from __future__ import annotations

import os
import argparse
import datetime as dt
import numpy as np
import pandas as pd
from loguru import logger
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()
PG_URI = os.getenv("POSTGRES_URI")

ISO = "%Y-%m-%d"

def rsi(series: pd.Series, window: int = 14) -> pd.Series:
    delta = series.diff()
    up = np.clip(delta, 0, None)
    down = -np.clip(delta, None, 0)
    roll_up = up.ewm(alpha=1/window, adjust=False).mean()
    roll_down = down.ewm(alpha=1/window, adjust=False).mean()
    rs = roll_up / roll_down.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def compute_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["ticker", "trade_date"]).copy()
    df["ret_1d"] = df.groupby("ticker")["close"].pct_change()
    # group computations
    feats = []
    for tkr, g in df.groupby("ticker", sort=False):
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
    out = pd.concat(feats).dropna(subset=["trade_date"])  # allow NaNs for early periods except trade_date
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True, help="YYYY-MM-DD (earliest date to compute)")
    ap.add_argument("--end", default="today", help="YYYY-MM-DD or 'today'")
    ap.add_argument("--universe-file", required=True, help="CSV with header 'ticker'")
    ap.add_argument("--delete-existing-window", type=int, default=0,
                    help="If >0, delete existing features for last N days before insert")
    args = ap.parse_args()

    start = dt.date.fromisoformat(args.start)
    end = dt.date.today() if args.end == "today" else dt.date.fromisoformat(args.end)

    if not PG_URI:
        raise SystemExit("POSTGRES_URI not set in .env")

    # load universe
    tickers = pd.read_csv(args.universe_file)["ticker"].astype(str).str.upper().tolist()

    eng = create_engine(PG_URI, future=True)

    # Pull prices from Postgres
    q = text("""
        SELECT ticker, trade_date, close, volume
        FROM core.prices_daily
        WHERE ticker = ANY(:tickers) AND trade_date BETWEEN :start AND :end
    """)
    with eng.connect() as con:
        df = pd.read_sql(q, con, params={"tickers": tickers, "start": start, "end": end})

    if df.empty:
        logger.warning("No price data returned for requested window.")
        return

    feats = compute_features(df)

    # Create schema and table first
    with eng.begin() as con:
        con.execute(text("CREATE SCHEMA IF NOT EXISTS core;"))
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
        
    # Optional: clean a small recent window to avoid duplicates
    if args.delete_existing_window and args.delete_existing_window > 0:
        cutoff = (end - dt.timedelta(days=args.delete_existing_window))
        with eng.begin() as con:
            con.execute(text("DELETE FROM core.features_technical_daily WHERE trade_date >= :cutoff"),
                        {"cutoff": cutoff})

    # Insert features row by row to handle conflicts
    with eng.begin() as con:
        for _, row in feats.iterrows():
            con.execute(
                text("""
                    INSERT INTO core.features_technical_daily (
                        ticker, trade_date, close, volume, rsi_14, sma_20, sma_50, sma_200,
                        ret_21d, ret_63d, vol_20d, breakout_252
                    ) VALUES (
                        :ticker, :trade_date, :close, :volume, :rsi_14, :sma_20, :sma_50, :sma_200,
                        :ret_21d, :ret_63d, :vol_20d, :breakout_252
                    )
                    ON CONFLICT (ticker, trade_date) DO UPDATE SET
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume,
                        rsi_14 = EXCLUDED.rsi_14,
                        sma_20 = EXCLUDED.sma_20,
                        sma_50 = EXCLUDED.sma_50,
                        sma_200 = EXCLUDED.sma_200,
                        ret_21d = EXCLUDED.ret_21d,
                        ret_63d = EXCLUDED.ret_63d,
                        vol_20d = EXCLUDED.vol_20d,
                        breakout_252 = EXCLUDED.breakout_252
                """),
                row.to_dict()
            )

    logger.success(f"Wrote {len(feats)} feature rows to core.features_technical_daily")

if __name__ == "__main__":
    main()