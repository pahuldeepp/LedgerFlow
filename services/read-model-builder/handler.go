package main

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// returns: processed=true if first time, processed=false if duplicate
func handleTransactionPosted(ctx context.Context, db *pgxpool.Pool, event TransactionPostedEvent) (bool, error) {
	tx, err := db.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	occurredAt, err := time.Parse(time.RFC3339Nano, event.OccurredAt)
	if err != nil {
		return false, err
	}

	// 1) Idempotent processing guard
	tag, err := tx.Exec(ctx, `
		INSERT INTO read_model.processed_events (event_id, consumer_name)
		VALUES ($1::uuid, 'read-model-builder')
		ON CONFLICT (event_id, consumer_name) DO NOTHING
	`, event.EventID)
	if err != nil {
		return false, err
	}

	// already processed
	if tag.RowsAffected() == 0 {
		if err := tx.Commit(ctx); err != nil {
			return false, err
		}
		return false, nil
	}

	// 2) Store payload in tx_feed
	payloadBytes, err := json.Marshal(event)
	if err != nil {
		return false, err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO read_model.tx_feed
			(event_id, transaction_id, occurred_at, payload)
		VALUES ($1::uuid, $2::uuid, $3, $4::jsonb)
		ON CONFLICT (event_id) DO NOTHING
	`, event.EventID, event.TransactionID, occurredAt, payloadBytes)
	if err != nil {
		return false, err
	}

	// 3) Update balances + account index
	for _, entry := range event.Entries {
		var delta int64

		switch entry.Direction {
		case "debit":
			delta = -entry.Amount
		case "credit":
			delta = entry.Amount
		default:
			return false, errors.New("invalid direction")
		}

		// balance
		_, err := tx.Exec(ctx, `
			INSERT INTO read_model.account_balances (account_id, balance, updated_at)
			VALUES ($1, $2, now())
			ON CONFLICT (account_id)
			DO UPDATE SET
				balance = read_model.account_balances.balance + EXCLUDED.balance,
				updated_at = now()
		`, entry.AccountID, delta)
		if err != nil {
			return false, err
		}

		// index
		_, err = tx.Exec(ctx, `
			INSERT INTO read_model.account_tx_index
				(account_id, occurred_at, event_id, transaction_id, direction, amount)
			VALUES ($1, $2, $3::uuid, $4::uuid, $5, $6)
			ON CONFLICT DO NOTHING
		`, entry.AccountID, occurredAt, event.EventID, event.TransactionID, entry.Direction, entry.Amount)
		if err != nil {
			return false, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return false, err
	}

	return true, nil
}
