-- Safe even if run multiple times
ALTER TABLE outbox
  ADD COLUMN IF NOT EXISTS locked_by TEXT,
  ADD COLUMN IF NOT EXISTS locked_at TIMESTAMPTZ;

-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_outbox_unpublished_ready
  ON outbox (created_at)
  WHERE published_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_outbox_locked
  ON outbox (locked_at)
  WHERE published_at IS NULL AND locked_by IS NOT NULL;