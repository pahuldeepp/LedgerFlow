CREATE SCHEMA IF NOT EXISTS read_model;

-- ==========================================================
-- 1) Deduplication Table (Idempotent Consumer)
-- ==========================================================
CREATE TABLE IF NOT EXISTS read_model.processed_events (
  event_id       uuid        NOT NULL,
  consumer_name  text        NOT NULL,
  processed_at   timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (event_id, consumer_name)
);

-- ==========================================================
-- 2) Account Balances Projection
-- ==========================================================
CREATE TABLE IF NOT EXISTS read_model.account_balances (
  account_id   text        PRIMARY KEY,
  balance      bigint      NOT NULL DEFAULT 0,
  updated_at   timestamptz NOT NULL DEFAULT now()
);

-- ==========================================================
-- 3) Global Transaction Feed
-- ==========================================================
CREATE TABLE IF NOT EXISTS read_model.tx_feed (
  event_id        uuid        PRIMARY KEY,
  transaction_id  uuid        NOT NULL,
  occurred_at     timestamptz NOT NULL,
  payload         jsonb       NOT NULL,
  created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_tx_feed_cursor
ON read_model.tx_feed (occurred_at DESC, event_id DESC);

-- ==========================================================
-- 4) Account → Transaction Index
-- ==========================================================
CREATE TABLE IF NOT EXISTS read_model.account_tx_index (
  account_id      text        NOT NULL,
  occurred_at     timestamptz NOT NULL,
  event_id        uuid        NOT NULL,
  transaction_id  uuid        NOT NULL,
  direction       text        NOT NULL CHECK (direction IN ('debit','credit')),
  amount          bigint      NOT NULL CHECK (amount > 0),
  created_at      timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (account_id, event_id)
);

CREATE INDEX IF NOT EXISTS idx_account_tx_cursor
ON read_model.account_tx_index (account_id, occurred_at DESC, event_id DESC);