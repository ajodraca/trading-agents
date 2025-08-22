CREATE DATABASE IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.prices_daily
(
  ticker         String,
  trade_date     Date,
  open           Float64,
  high           Float64,
  low            Float64,
  close          Float64,
  volume         UInt64,
  vwap           Float64,
  adjusted_close Float64
)
ENGINE = MergeTree
ORDER BY (ticker, trade_date);