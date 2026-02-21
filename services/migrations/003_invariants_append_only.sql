-- ===========================================
-- 003_invariants_append_only.sql
-- Enforce: (1) append-only (no UPDATE/DELETE)
--          (2) no cascading deletes
--          (3) balanced transaction invariant at COMMIT
-- ===========================================

BEGIN;

-- 1) Remove ON DELETE CASCADE from entries -> transactions
ALTER TABLE public.entries
  DROP CONSTRAINT IF EXISTS entries_transaction_id_fkey;

ALTER TABLE public.entries
  ADD CONSTRAINT entries_transaction_id_fkey
  FOREIGN KEY (transaction_id) REFERENCES public.transactions(id)
  ON DELETE RESTRICT;

-- 2) Append-only: block UPDATE/DELETE on ledger tables
CREATE OR REPLACE FUNCTION public.block_update_delete()
RETURNS trigger AS $$
BEGIN
  RAISE EXCEPTION 'append-only table: % operation is not allowed', TG_TABLE_NAME;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_transactions_block_ud ON public.transactions;
CREATE TRIGGER trg_transactions_block_ud
BEFORE UPDATE OR DELETE ON public.transactions
FOR EACH ROW EXECUTE FUNCTION public.block_update_delete();

DROP TRIGGER IF EXISTS trg_entries_block_ud ON public.entries;
CREATE TRIGGER trg_entries_block_ud
BEFORE UPDATE OR DELETE ON public.entries
FOR EACH ROW EXECUTE FUNCTION public.block_update_delete();

-- NOTE: outbox needs UPDATE (published_at/attempts), so we do NOT block it.

-- 3) Ledger invariant: debits == credits per transaction (DB-enforced)
CREATE OR REPLACE FUNCTION public.assert_transaction_balanced(p_tx_id uuid)
RETURNS void AS $$
DECLARE
  debit_sum  bigint;
  credit_sum bigint;
BEGIN
  SELECT COALESCE(SUM(amount), 0) INTO debit_sum
  FROM public.entries
  WHERE transaction_id = p_tx_id AND direction = 'debit';

  SELECT COALESCE(SUM(amount), 0) INTO credit_sum
  FROM public.entries
  WHERE transaction_id = p_tx_id AND direction = 'credit';

  IF debit_sum <> credit_sum THEN
    RAISE EXCEPTION 'ledger invariant violated for tx %: debit=% credit=%',
      p_tx_id, debit_sum, credit_sum;
  END IF;

  -- Optional “minimum 2 entries” invariant (keeps it ledger-like)
  IF (SELECT COUNT(*) FROM public.entries WHERE transaction_id = p_tx_id) < 2 THEN
    RAISE EXCEPTION 'ledger invariant violated for tx %: requires >= 2 entries', p_tx_id;
  END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION public.entries_balance_trigger()
RETURNS trigger AS $$
BEGIN
  PERFORM public.assert_transaction_balanced(NEW.transaction_id);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_entries_balance ON public.entries;

CREATE CONSTRAINT TRIGGER trg_entries_balance
AFTER INSERT ON public.entries
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION public.entries_balance_trigger();

COMMIT;
