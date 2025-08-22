CREATE SCHEMA IF NOT EXISTS core;
CREATE TABLE IF NOT EXISTS core.rankings_top100 (
  run_id           TEXT        NOT NULL,
  agent_name       TEXT        NOT NULL,  -- 'technical' or 'fundamental'
  as_of_ts         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  trade_date       DATE        NOT NULL,
  ticker           TEXT        NOT NULL,
  rank             INT         NOT NULL,
  score            DOUBLE PRECISION,
  explanation_json JSONB,
  PRIMARY KEY (run_id, agent_name, rank)
);
CREATE INDEX IF NOT EXISTS idx_rankings_latest ON core.rankings_top100 (agent_name, trade_date DESC);

