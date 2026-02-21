create schema if not exists billing;

create extension if not exists "pgcrypto";


CREATE TABLE IF NOT EXISTS billing.invoices (
  invoice_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  transaction_id uuid NOT NULL,
  amount bigint NOT NULL,
  status text NOT NULL DEFAULT 'created',
  created_at timestamptz NOT NULL DEFAULT now()
);



CREATE UNIQUE INDEX IF NOT EXISTS ux_billing_transaction
ON billing.invoices(transaction_id);


CREATE TABLE IF NOT EXISTS billing.outbox (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  event_type text NOT NULL,
  payload jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  published_at timestamptz NULL,
  attempts int NOT NULL DEFAULT 0,
  next_attempt_at timestamptz NOT NULL DEFAULT now(),
  last_error text NULL
);


CREATE INDEX IF NOT EXISTS idx_billing_outbox_pending
ON billing.outbox (published_at, next_attempt_at, created_at);