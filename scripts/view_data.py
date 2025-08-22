"""Script to view data stored in the database."""

import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

def main():
    # Connect to database
    engine = create_engine(os.getenv("POSTGRES_URI"), future=True)
    
    # Query prices
    with engine.connect() as conn:
        # Get total count of price records
        price_count = pd.read_sql("""
            SELECT COUNT(*) as count 
            FROM core.prices_daily
        """, conn).iloc[0]['count']
        
        print(f"\nTotal price records: {price_count}")
        
        # Sample of recent prices
        print("\nRecent prices sample:")
        prices = pd.read_sql("""
            SELECT ticker, trade_date, close, adjusted_close
            FROM core.prices_daily
            WHERE trade_date >= CURRENT_DATE - INTERVAL '5 days'
            ORDER BY trade_date DESC, ticker
            LIMIT 10
        """, conn)
        print(prices)
        
        # Get corporate actions
        print("\nRecent corporate actions:")
        actions = pd.read_sql("""
            SELECT ticker, ca_type, ex_date, 
                   CASE 
                     WHEN ca_type = 'split' THEN split_from::text || ':' || split_to::text
                     ELSE amount::text
                   END as value
            FROM core.corporate_actions
            WHERE ex_date >= '2024-01-01'
            ORDER BY ex_date DESC, ticker
            LIMIT 10
        """, conn)
        print(actions)

if __name__ == "__main__":
    main()
