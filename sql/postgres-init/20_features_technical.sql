CREATE SCHEMA IF NOT EXISTS core;
CREATE TABLE IF NOT EXISTS core.features_technical_daily (
  ticker       TEXT        NOT NULL,
  trade_date   DATE        NOT NULL,
  close        DOUBLE PRECISION,
  volume       BIGINT,
  rsi_14       DOUBLE PRECISION,
  sma_20       DOUBLE PRECISION,
  sma_50       DOUBLE PRECISION,
  sma_200      DOUBLE PRECISION,
  ret_21d      DOUBLE PRECISION,  -- ~1 month trading days
  ret_63d      DOUBLE PRECISION,  -- ~3 months trading days
  vol_20d      DOUBLE PRECISION,  -- stdev of daily returns
  breakout_252 BOOLEAN,
  PRIMARY KEY (ticker, trade_date)
);
