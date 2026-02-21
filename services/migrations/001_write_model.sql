-- =========================
-- ENTRIES (Double Entry Ledger)
-- =========================
CREATE TABLE IF NOT EXISTS entries (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  transaction_id UUID NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
  account_id TEXT NOT NULL REFERENCES accounts(id),
  direction TEXT NOT NULL CHECK (direction IN ('debit','credit')),
  amount BIGINT NOT NULL CHECK (amount > 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_entries_tx ON entries(transaction_id);
CREATE INDEX IF NOT EXISTS idx_entries_account ON entries(account_id);

-- =========================
-- OUTBOX
-- =========================
CREATE TABLE IF NOT EXISTS outbox (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  aggregate_type TEXT NOT NULL,
  aggregate_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  payload JSONB NOT NULL,

  attempts INT NOT NULL DEFAULT 0,
  next_attempt_at TIMESTAMPTZ NULL,
  last_error TEXT NULL,

  published_at TIMESTAMPTZ NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_outbox_ready
ON outbox (published_at, next_attempt_at, created_at);

CREATE INDEX IF NOT EXISTS idx_outbox_unpublished
ON outbox (published_at)
WHERE published_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_outbox_next_attempt
ON outbox (next_attempt_at);

CREATE INDEX IF NOT EXISTS idx_outbox_created
ON outbox (created_at);