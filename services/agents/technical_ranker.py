#!/usr/bin/env python3
from __future__ import annotations
import os
import argparse
import uuid
import datetime as dt
import numpy as np
import pandas as pd
from loguru import logger
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
load_dotenv()
PG_URI = os.getenv("POSTGRES_URI")
WEIGHTS = {
    "z_ret_63d": 0.35,
    "z_ret_21d": 0.25,
    "z_trend_200": 0.20,  # close / sma200 - 1
    "z_rsi": 0.10,
    "breakout": 0.10,
}
def zscore(s: pd.Series) -> pd.Series:
    return (s - s.mean()) / (s.std(ddof=0) if s.std(ddof=0) != 0 else 1)
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--as-of", default="latest", help="YYYY-MM-DD (trade date) or 'latest'")
    ap.add_argument("--universe-file", required=True, help="CSV with header 'ticker'")
    args = ap.parse_args()

    if not PG_URI:
        raise SystemExit("POSTGRES_URI not set in .env")

    eng = create_engine(PG_URI, future=True)
    
    # Determine as-of date (latest available if not provided)
    with eng.connect() as con:
        if args.as_of == "latest":
            asof = con.execute(text("SELECT MAX(trade_date) FROM core.features_technical_daily")).scalar_one()
        else:
            asof = dt.date.fromisoformat(args.as_of)
    # Pull features for that date across the universe
    tickers = pd.read_csv(args.universe_file)["ticker"].astype(str).str.upper().tolist()
    q = text("""
        SELECT f.ticker, f.trade_date, f.close, f.rsi_14, f.sma_200, f.ret_21d, f.ret_63d, f.breakout_252
        FROM core.features_technical_daily f
        WHERE f.trade_date = :asof AND f.ticker = ANY(:tickers)
    """)
    with eng.connect() as con:
        df = pd.read_sql(q, con, params={"asof": asof, "tickers": tickers})

    if df.empty:
        logger.error("No features for the selected date. Run features job first.")
        return

    # Build factors
    df["trend_200"] = df["close"] / df["sma_200"] - 1.0
    df["z_ret_21d"] = zscore(df["ret_21d"].fillna(0))
    df["z_ret_63d"] = zscore(df["ret_63d"].fillna(0))
    df["z_trend_200"] = zscore(df["trend_200"].fillna(0))
    df["z_rsi"] = zscore((df["rsi_14"].clip(0,100) / 100.0).fillna(0))
    df["breakout"] = df["breakout_252"].astype(float)

    # Composite score
    df["score"] = (
        WEIGHTS["z_ret_63d"] * df["z_ret_63d"] +
        WEIGHTS["z_ret_21d"] * df["z_ret_21d"] +
        WEIGHTS["z_trend_200"] * df["z_trend_200"] +
        WEIGHTS["z_rsi"] * df["z_rsi"] +
        WEIGHTS["breakout"] * df["breakout"]
    )

    df = df.sort_values("score", ascending=False).reset_index(drop=True)
    df["rank"] = np.arange(1, len(df) + 1)
    top100 = df.head(100).copy()
    run_id = str(uuid.uuid4())
# Persist
    with eng.begin() as con:
        con.execute(text("""
            CREATE TABLE IF NOT EXISTS core.rankings_top100 (
              run_id           TEXT        NOT NULL,
              agent_name       TEXT        NOT NULL,
              as_of_ts         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
              trade_date       DATE        NOT NULL,
              ticker           TEXT        NOT NULL,
              rank             INT         NOT NULL,
              score            DOUBLE PRECISION,
              explanation_json JSONB,
              PRIMARY KEY (run_id, agent_name, rank)
            );
        """))
    # Build a tiny rationale for transparency and prepare for JSONB
    top100["explanation_json"] = top100.apply(
        lambda row: {
            "z_ret_63d": round(float(row["z_ret_63d"]), 2),
            "z_ret_21d": round(float(row["z_ret_21d"]), 2),
            "z_trend_200": round(float(row["z_trend_200"]), 2),
            "z_rsi": round(float(row["z_rsi"]), 2),
            "breakout": round(float(row["breakout"]), 2)
        },
        axis=1
    )

    out = top100[[
        "ticker", "rank", "score", "explanation_json"
    ]].copy()
    out.insert(0, "trade_date", asof)
    out.insert(0, "agent_name", "technical")
    out.insert(0, "run_id", run_id)

    # Insert using parameterized query with explicit cast function
    with eng.begin() as con:
        # First create a function to cast JSON text to JSONB if it doesn't exist
        con.execute(text("""
            CREATE OR REPLACE FUNCTION cast_to_jsonb(text) RETURNS jsonb AS $$
            BEGIN
                RETURN $1::jsonb;
            EXCEPTION WHEN OTHERS THEN
                RETURN '{}'::jsonb;
            END;
            $$ LANGUAGE plpgsql;
        """))
        
        # Now use the function in our insert
        for _, record in out.iterrows():
            json_str = str(record["explanation_json"]).replace("'", '"')
            con.execute(text("""
                INSERT INTO core.rankings_top100 
                (run_id, agent_name, trade_date, ticker, rank, score, explanation_json)
                VALUES 
                (:run_id, :agent_name, :trade_date, :ticker, :rank, :score, cast_to_jsonb(:explanation_json))
            """), {
                "run_id": record["run_id"],
                "agent_name": record["agent_name"],
                "trade_date": record["trade_date"],
                "ticker": record["ticker"],
                "rank": record["rank"],
                "score": record["score"],
                "explanation_json": json_str
            })

    logger.success(f"Inserted Top-100 technical ranking for {asof} with run_id={run_id}")

if __name__ == "__main__":
    main()
