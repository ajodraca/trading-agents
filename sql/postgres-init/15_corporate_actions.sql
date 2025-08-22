CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.corporate_actions (
  ticker           TEXT        NOT NULL,
  ca_type          TEXT        NOT NULL,  -- 'split' or 'dividend'
  ex_date          DATE,                 -- execution_date for splits; ex_dividend_date for dividends
  record_date      DATE,
  payable_date     DATE,
  declared_date    DATE,
  split_from       NUMERIC(18,8),        -- for splits
  split_to         NUMERIC(18,8),        -- for splits
  amount           NUMERIC(18,8),        -- dividend cash amount
  frequency        TEXT,                 -- e.g., 0,1,2,4,12 per Polygon docs
  source           TEXT DEFAULT 'polygon',
  PRIMARY KEY (ticker, ca_type, ex_date)
);