CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- =========================
-- ACCOUNTS
-- =========================
CREATE TABLE IF NOT EXISTS accounts (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- =========================
-- TRANSACTIONS
-- =========================
CREATE TABLE IF NOT EXISTS transactions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  idempotency_key TEXT UNIQUE NOT NULL,
  status TEXT NOT NULL DEFAULT 'posted',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- =========================
-- SEED DATA
-- =========================
INSERT INTO accounts (id, name)
VALUES
  ('acc_1', 'Cash'),
  ('acc_2', 'Revenue')
ON CONFLICT (id) DO NOTHING;