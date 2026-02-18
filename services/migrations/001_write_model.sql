CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ===============================
-- WRITE MODEL
-- ===============================

CREATE TABLE transactions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    idempotency_key TEXT UNIQUE NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE entries (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    transaction_id UUID NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
    account_id TEXT NOT NULL,
    direction TEXT NOT NULL CHECK (direction IN ('debit','credit')),
    amount BIGINT NOT NULL CHECK (amount > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_entries_tx ON entries(transaction_id);
CREATE INDEX idx_entries_account ON entries(account_id);

CREATE TABLE outbox (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    aggregate_type TEXT NOT NULL,
    aggregate_id UUID NOT NULL,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    published_at TIMESTAMPTZ,
    attempts INT NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ
);

CREATE INDEX idx_outbox_pending
ON outbox (published_at, next_attempt_at);

CREATE INDEX idx_outbox_created
ON outbox (created_at);
