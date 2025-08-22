#!/usr/bin/env python3
"""Technical ranking agent that scores stocks based on momentum and trend indicators."""
from __future__ import annotations

import os
import sys
import json
import uuid
import argparse
import datetime as dt
from typing import Dict, List

import numpy as np
import pandas as pd
from loguru import logger
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

load_dotenv()

# Configuration
PG_URI = os.getenv("POSTGRES_URI")
DEFAULT_WEIGHTS = {
    "z_ret_63d": 0.35,    # 63-day return (momentum)
    "z_ret_21d": 0.25,    # 21-day return (momentum)
    "z_trend_200": 0.20,  # Trend relative to 200-day MA
    "z_rsi": 0.10,        # RSI momentum
    "breakout": 0.10,     # 52-week high breakout
}

def zscore(s: pd.Series) -> pd.Series:
    """Compute z-score of a series.
    
    Args:
        s: Input series
    
    Returns:
        Z-scored series
    """
    return (s - s.mean()) / (s.std(ddof=0) if s.std(ddof=0) != 0 else 1)

def validate_universe_file(filepath: str) -> List[str]:
    """Load and validate universe of tickers.
    
    Args:
        filepath: Path to CSV file with ticker column
    
    Returns:
        List of tickers
    
    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If file format is invalid
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Universe file not found: {filepath}")
    
    try:
        tickers = pd.read_csv(filepath)["ticker"].astype(str).str.upper().tolist()
    except Exception as e:
        raise ValueError(f"Invalid universe file format: {e}")
    
    if not tickers:
        raise ValueError("Empty universe file")
    
    return tickers

def get_asof_date(eng, specified_date: str = "latest") -> dt.date:
    """Get the as-of date for ranking.
    
    Args:
        eng: Database engine
        specified_date: Date string or 'latest'
    
    Returns:
        Date to use for ranking
    
    Raises:
        ValueError: If date is invalid or no data available
    """
    with eng.connect() as con:
        if specified_date == "latest":
            asof = con.execute(
                text("SELECT MAX(trade_date) FROM core.features_technical_daily")
            ).scalar_one()
            if not asof:
                raise ValueError("No feature data available")
        else:
            try:
                asof = dt.date.fromisoformat(specified_date)
            except ValueError:
                raise ValueError(f"Invalid date format: {specified_date}")
            
            # Verify data exists for this date
            exists = con.execute(
                text("SELECT 1 FROM core.features_technical_daily WHERE trade_date = :dt LIMIT 1"),
                {"dt": asof}
            ).scalar_one_or_none()
            
            if not exists:
                raise ValueError(f"No feature data available for {asof}")
    
    return asof

def get_features(eng, asof: dt.date, tickers: List[str]) -> pd.DataFrame:
    """Get technical features for ranking.
    
    Args:
        eng: Database engine
        asof: As-of date
        tickers: List of tickers to rank
    
    Returns:
        DataFrame with features
    """
    query = text("""
        SELECT 
            f.ticker, f.trade_date, f.close, f.rsi_14, 
            f.sma_200, f.ret_21d, f.ret_63d, f.breakout_252
        FROM core.features_technical_daily f
        WHERE f.trade_date = :asof AND f.ticker = ANY(:tickers)
    """)
    
    with eng.connect() as con:
        df = pd.read_sql(query, con, params={"asof": asof, "tickers": tickers})
    
    if df.empty:
        raise ValueError(f"No features available for {asof}")
    
    return df

def compute_scores(
    df: pd.DataFrame, 
    weights: Dict[str, float] = DEFAULT_WEIGHTS
) -> pd.DataFrame:
    """Compute composite scores from features.
    
    Args:
        df: DataFrame with raw features
        weights: Factor weights
    
    Returns:
        DataFrame with scores and ranks added
    """
    # Verify required columns
    required = {"close", "rsi_14", "sma_200", "ret_21d", "ret_63d", "breakout_252"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required features: {missing}")

    # Build factors
    df = df.copy()
    df["trend_200"] = df["close"] / df["sma_200"] - 1.0
    df["z_ret_21d"] = zscore(df["ret_21d"].fillna(0))
    df["z_ret_63d"] = zscore(df["ret_63d"].fillna(0))
    df["z_trend_200"] = zscore(df["trend_200"].fillna(0))
    df["z_rsi"] = zscore((df["rsi_14"].clip(0,100) / 100.0).fillna(0))
    df["breakout"] = df["breakout_252"].astype(float)

    # Composite score
    df["score"] = (
        weights["z_ret_63d"] * df["z_ret_63d"] +
        weights["z_ret_21d"] * df["z_ret_21d"] +
        weights["z_trend_200"] * df["z_trend_200"] +
        weights["z_rsi"] * df["z_rsi"] +
        weights["breakout"] * df["breakout"]
    )

    # Rank
    df = df.sort_values("score", ascending=False).reset_index(drop=True)
    df["rank"] = np.arange(1, len(df) + 1)
    
    return df

def format_rankings(
    df: pd.DataFrame, 
    asof: dt.date,
    run_id: str,
    top_n: int = 100
) -> pd.DataFrame:
    """Format rankings for storage.
    
    Args:
        df: DataFrame with scores
        asof: As-of date
        run_id: Unique run identifier
        top_n: Number of top ranks to keep
    
    Returns:
        DataFrame formatted for database storage
    """
    top = df.head(top_n).copy()
    
    # Build explanations
    factors = ["z_ret_63d", "z_ret_21d", "z_trend_200", "z_rsi", "breakout"]
    top["explanation_json"] = top[factors].apply(
        lambda x: json.dumps({f: round(float(x[f]), 2) for f in factors})
    )
    
    # Format output
    out = top[["ticker", "rank", "score", "explanation_json"]].copy()
    out.insert(0, "trade_date", asof)
    out.insert(0, "agent_name", "technical")
    out.insert(0, "run_id", run_id)
    
    return out

def store_rankings(eng, rankings: pd.DataFrame) -> None:
    """Store rankings in database.
    
    Args:
        eng: Database engine
        rankings: DataFrame with rankings to store
    """
    with eng.begin() as con:
        # Ensure table exists
        con.execute(text("""
            CREATE TABLE IF NOT EXISTS core.rankings_top100 (
              run_id           TEXT        NOT NULL,
              agent_name      TEXT        NOT NULL,
              as_of_ts        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
              trade_date      DATE        NOT NULL,
              ticker          TEXT        NOT NULL,
              rank           INT         NOT NULL,
              score          DOUBLE PRECISION,
              explanation_json JSONB,
              PRIMARY KEY (run_id, agent_name, rank)
            );
        """))
        
        # Use temp table for efficient insert
        con.execute(text("CREATE TEMP TABLE tmp_rank (LIKE core.rankings_top100 INCLUDING ALL);"))
        rankings.to_sql("tmp_rank", con.connection, if_exists="append", index=False)
        con.execute(text("""
            INSERT INTO core.rankings_top100 (
                run_id, agent_name, trade_date, ticker, 
                rank, score, explanation_json
            )
            SELECT 
                run_id, agent_name, trade_date, ticker,
                rank, score, explanation_json::jsonb
            FROM tmp_rank;
        """))

def main():
    # Parse arguments
    ap = argparse.ArgumentParser(description="Generate technical rankings for a universe of stocks")
    ap.add_argument(
        "--as-of", 
        default="latest", 
        help="YYYY-MM-DD (trade date) or 'latest'"
    )
    ap.add_argument(
        "--universe-file", 
        required=True, 
        help="CSV with header 'ticker'"
    )
    args = ap.parse_args()

    # Validate environment
    if not PG_URI:
        raise SystemExit("POSTGRES_URI not set in .env")

    try:
        # Initialize
        eng = create_engine(PG_URI, future=True)
        run_id = str(uuid.uuid4())
        logger.info(f"Starting ranking run {run_id}")
        
        # Load inputs
        tickers = validate_universe_file(args.universe_file)
        logger.info(f"Loaded {len(tickers)} tickers from universe")
        
        asof = get_asof_date(eng, args.as_of)
        logger.info(f"Ranking as of {asof}")
        
        # Get features and compute ranks
        features = get_features(eng, asof, tickers)
        logger.info(f"Got features for {len(features)} stocks")
        
        scored = compute_scores(features)
        logger.info("Computed scores and ranks")
        
        # Format and store
        rankings = format_rankings(scored, asof, run_id)
        store_rankings(eng, rankings)
        
        logger.success(
            f"Completed run {run_id}: Generated rankings for {len(rankings)} stocks as of {asof}"
        )
        
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Input error: {e}")
        sys.exit(1)
    except SQLAlchemyError as e:
        logger.error(f"Database error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise

if __name__ == "__main__":
    main()
