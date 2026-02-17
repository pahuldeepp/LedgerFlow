package main

import (
	"context"
	"log"
	"time"
)

const (
	maxAttempts    = 5
	maxBackoffSecs = 60
	batchSize      = 10
	pollInterval   = 1 * time.Second
)

type outboxItem struct {
	id        string
	eventType string
	payload   string
}

func startOutboxWorker() {
	go func() {
		ticker := time.NewTicker(pollInterval)
		defer ticker.Stop()

		for range ticker.C {
			if err := processOutboxBatch(); err != nil {
				log.Println("outbox worker error:", err)
			}
		}
	}()
}

func processOutboxBatch() error {
	ctx := context.Background()

	// Best-effort: update pending gauge (don’t fail worker if it errors)
	if outboxPending != nil {
		var cnt int64
		_ = db.QueryRow(ctx, `
			SELECT COUNT(*)
			FROM outbox
			WHERE published_at IS NULL
			  AND attempts < $1
			  AND (next_attempt_at IS NULL OR next_attempt_at <= NOW())
		`, maxAttempts).Scan(&cnt)
		outboxPending.Set(float64(cnt))
	}

	// 1) Lock a batch fast, then release locks
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT id::text, event_type, payload::text
		FROM outbox
		WHERE published_at IS NULL
		  AND attempts < $1
		  AND (next_attempt_at IS NULL OR next_attempt_at <= NOW())
		ORDER BY created_at ASC
		FOR UPDATE SKIP LOCKED
		LIMIT $2
	`, maxAttempts, batchSize)
	if err != nil {
		return err
	}
	defer rows.Close()

	batch := make([]outboxItem, 0, batchSize)
	for rows.Next() {
		var it outboxItem
		if err := rows.Scan(&it.id, &it.eventType, &it.payload); err != nil {
			return err
		}
		batch = append(batch, it)
	}

	// release row locks ASAP
	if err := tx.Commit(ctx); err != nil {
		return err
	}

	// 2) Publish outside DB tx (network I/O must not hold locks)
	for _, it := range batch {
		if err := publishToKafka(it.id, []byte(it.payload)); err != nil {
			if outboxPublishFailedTotal != nil {
				outboxPublishFailedTotal.Inc()
			}

			var attempts int
			err2 := db.QueryRow(ctx, `
				UPDATE outbox
				SET attempts = attempts + 1,
				    last_error = $2,
				    next_attempt_at = NOW() + make_interval(
				        secs => LEAST(POWER(2, attempts), $3)
				    )
				WHERE id = $1
				RETURNING attempts
			`, it.id, err.Error(), maxBackoffSecs).Scan(&attempts)

			if err2 != nil {
				log.Println("outbox: failed to record publish failure:", err2)
				continue
			}

			// attempts just got incremented; if we hit max, move to DLQ
			if attempts >= maxAttempts {
				if err := moveToDLQ(ctx, it.id); err != nil {
					log.Println("outbox: failed to move to DLQ:", err)
				} else if outboxDLQTotal != nil {
					outboxDLQTotal.Inc()
				}
			}
			continue
		}

		// success: mark published
		_, err := db.Exec(ctx, `
			UPDATE outbox
			SET published_at = NOW(),
			    last_error = NULL
			WHERE id = $1
		`, it.id)
		if err != nil {
			log.Println("outbox: failed to mark published:", err)
			continue
		}

		if outboxPublishedTotal != nil {
			outboxPublishedTotal.Inc()
		}
	}

	return nil
}

func moveToDLQ(ctx context.Context, id string) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `
		INSERT INTO outbox_dlq (id, aggregate_type, aggregate_id, event_type, payload, created_at, attempts, last_error)
		SELECT id, aggregate_type, aggregate_id, event_type, payload, created_at, attempts, last_error
		FROM outbox
		WHERE id = $1
	`, id)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `DELETE FROM outbox WHERE id = $1`, id)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}
