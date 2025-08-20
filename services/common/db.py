import os
from urllib.parse import urlparse
from sqlalchemy import create_engine, text
from clickhouse_connect import get_client
from dotenv import load_dotenv

load_dotenv()

PG_URI = os.getenv("POSTGRES_URI")
CH_URL = os.getenv("CLICKHOUSE_URL", "http://localhost:8123")

def test_postgres():
    engine = create_engine(PG_URI, future=True)
    with engine.connect() as conn:
        conn.execute(text("CREATE TABLE IF NOT EXISTS core.healthcheck (ts TIMESTAMPTZ DEFAULT NOW());"))
        conn.execute(text("INSERT INTO core.healthcheck DEFAULT VALUES;"))
        row = conn.execute(text("SELECT COUNT(*) FROM core.healthcheck;")).scalar_one()
        print(f"Postgres OK. healthcheck rows: {row}")

def test_clickhouse():
    parsed = urlparse(CH_URL)
    client = get_client(host=parsed.hostname or "localhost", port=parsed.port or 8123)
    client.command("CREATE DATABASE IF NOT EXISTS core;")
    client.command("""
        CREATE TABLE IF NOT EXISTS core.healthcheck
        (ts DateTime DEFAULT now())
        ENGINE = MergeTree ORDER BY ts
    """)
    client.command("INSERT INTO core.healthcheck VALUES ()")
    row = client.query("SELECT count() FROM core.healthcheck").result_rows[0][0]
    print(f"ClickHouse OK. healthcheck rows: {row}")

if __name__ == "__main__":
    test_postgres()
    test_clickhouse()