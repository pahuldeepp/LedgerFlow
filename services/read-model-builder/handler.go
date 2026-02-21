package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func handleTransactionPosted(ctx context.Context, db *pgxpool.Pool, ev TransactionPostedEvent) (bool, error) {
	tx, err := db.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// 1️⃣ Idempotency gate (composite PK: event_id + consumer_name)
	inserted, err := markProcessedIfNew(ctx, tx, ev.EventID)
	if err != nil {
		return false, err
	}
	if !inserted {
		// duplicate event for this consumer
		if err := tx.Commit(ctx); err != nil {
			return false, err
		}
		return false, nil
	}

	// 2️⃣ Apply projection writes atomically
	if err := applyTransactionProjection(ctx, tx, ev); err != nil {
		return false, err
	}

	// 3️⃣ Commit
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}

	return true, nil
}

func markProcessedIfNew(ctx context.Context, tx pgx.Tx, eventID string) (bool, error) {
	const consumerName = "read-model-builder"

	var inserted string
	err := tx.QueryRow(ctx, `
		INSERT INTO read_model.processed_events (event_id, consumer_name, processed_at)
		VALUES ($1, $2, now())
		ON CONFLICT (event_id, consumer_name) DO NOTHING
		RETURNING event_id
	`, eventID, consumerName).Scan(&inserted)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil // duplicate
		}
		return false, err
	}
	return true, nil
}

func applyTransactionProjection(ctx context.Context, tx pgx.Tx, ev TransactionPostedEvent) error {
	if ev.EventID == "" || ev.TransactionID == "" {
		return fmt.Errorf("missing event_id or transaction_id")
	}

	if ev.OccurredAt == "" {
		ev.OccurredAt = time.Now().UTC().Format(time.RFC3339Nano)
	}

	// 🔹 Validate double-entry invariants
	var creditTotal, debitTotal int64
	for _, e := range ev.Entries {
		switch strings.ToLower(e.Direction) {
		case "credit":
			creditTotal += e.Amount
		case "debit":
			debitTotal += e.Amount
		default:
			return fmt.Errorf("invalid direction: %s", e.Direction)
		}
	}

	if creditTotal != debitTotal {
		return fmt.Errorf(
			"unbalanced tx: credit=%d debit=%d tx=%s",
			creditTotal,
			debitTotal,
			ev.TransactionID,
		)
	}

	// 🔹 Insert into tx_feed (MATCHES MIGRATION)
	payloadBytes, err := json.Marshal(ev)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO read_model.tx_feed
			(event_id, transaction_id, occurred_at, payload)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (event_id) DO NOTHING
	`, ev.EventID, ev.TransactionID, ev.OccurredAt, payloadBytes)
	if err != nil {
		return err
	}

	// 🔹 Per-entry projections
	for _, e := range ev.Entries {
		dir := strings.ToLower(e.Direction)

		if e.Amount <= 0 {
			return fmt.Errorf("amount must be > 0")
		}

		// 1️⃣ account_tx_index (MATCHES SCHEMA)
		_, err = tx.Exec(ctx, `
			INSERT INTO read_model.account_tx_index
				(account_id, occurred_at, event_id, transaction_id, direction, amount)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (account_id, event_id) DO NOTHING
		`, e.AccountID, ev.OccurredAt, ev.EventID, ev.TransactionID, dir, e.Amount)
		if err != nil {
			return err
		}

		// 2️⃣ account_balances
		delta := e.Amount
		if dir == "debit" {
			delta = -delta
		}

		_, err = tx.Exec(ctx, `
			INSERT INTO read_model.account_balances (account_id, balance)
			VALUES ($1, $2)
			ON CONFLICT (account_id)
			DO UPDATE SET
				balance = read_model.account_balances.balance + EXCLUDED.balance,
				updated_at = now()
		`, e.AccountID, delta)
		if err != nil {
			return err
		}
	}

	return nil
}
