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