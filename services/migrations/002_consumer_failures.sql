CREATE SCHEMA IF NOT EXISTS read_model;

CREATE TABLE IF NOT EXISTS read_model.consumer_failures (
  event_id TEXT PRIMARY KEY,
  retry_count INT NOT NULL DEFAULT 0,
  next_retry_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_error TEXT NOT NULL DEFAULT '',
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_consumer_failures_next_retry_at
  ON read_model.consumer_failures (next_retry_at);