package main

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func handleTransactionPosted(ctx context.Context, db *pgxpool.Pool, event TransactionPostedEvent) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// Parse occurred_at string → time.Time
	occurredAt, err := time.Parse(time.RFC3339Nano, event.OccurredAt)
	if err != nil {
		return err
	}

	// 1️⃣ DEDUPE (exactly-once-ish)
	tag, err := tx.Exec(ctx, `
		INSERT INTO read_model.processed_events (event_id, consumer_name)
		VALUES ($1, 'read-model-builder')
		ON CONFLICT (event_id, consumer_name) DO NOTHING
	`, event.EventID)
	if err != nil {
		return err
	}

	if tag.RowsAffected() == 0 {
		return tx.Commit(ctx) // already processed
	}

	// 2️⃣ UPDATE BALANCES
	for _, entry := range event.Entries {
		var delta int64

		switch entry.Direction {
		case "debit":
			delta = -entry.Amount
		case "credit":
			delta = entry.Amount
		default:
			return errors.New("invalid entry.direction: " + entry.Direction)
		}

		_, err := tx.Exec(ctx, `
			INSERT INTO read_model.account_balances
				(account_id, balance, updated_at)
			VALUES ($1, $2, now())
			ON CONFLICT (account_id)
			DO UPDATE SET
				balance = read_model.account_balances.balance + EXCLUDED.balance,
				updated_at = now()
		`, entry.AccountID, delta)

		if err != nil {
			return err
		}
	}

	// 3️⃣ TX FEED
	_, err = tx.Exec(ctx, `
		INSERT INTO read_model.tx_feed
			(tx_id, created_at, amount, status)
		VALUES ($1, $2, $3, 'posted')
		ON CONFLICT (tx_id) DO NOTHING
	`,
		event.TransactionID,
		occurredAt,
		event.TotalAmount(),
	)
	if err != nil {
		return err
	}

	// 4️⃣ ACCOUNT -> TX INDEX
	for _, entry := range event.Entries {
		_, err := tx.Exec(ctx, `
			INSERT INTO read_model.account_tx_index
				(account_id, created_at, tx_id)
			VALUES ($1, $2, $3)
			ON CONFLICT DO NOTHING
		`,
			entry.AccountID,
			occurredAt,
			event.TransactionID,
		)

		if err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}
