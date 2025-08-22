#!/usr/bin/env python3
from __future__ import annotations
import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
PG_URI = os.getenv("POSTGRES_URI")

def main():
    if not PG_URI:
        raise SystemExit("POSTGRES_URI not set in .env")

    eng = create_engine(PG_URI)
    
    # Get the latest rankings
    query = text("""
        WITH latest_date AS (
            SELECT MAX(trade_date) as max_date 
            FROM core.rankings_top100
        )
        SELECT 
            r.rank,
            r.ticker,
            r.score,
            r.explanation_json
        FROM core.rankings_top100 r
        JOIN latest_date ld ON r.trade_date = ld.max_date
        ORDER BY r.rank
        LIMIT 10;
    """)
    
    with eng.connect() as con:
        df = pd.read_sql(query, con)
    
    # Display top 10 stocks with their scores and explanations
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    print("\n=== Top 10 Technical Rankings ===")
    print(df)

if __name__ == "__main__":
    main()
