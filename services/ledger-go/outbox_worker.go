package main

import (
	"context"
	"errors"
	"time"

	"go.uber.org/zap"
)

const (
	maxAttempts    = 5
	maxBackoffSecs = 60
	batchSize      = 10
	pollInterval   = 1 * time.Second
)

// Circuit breaker: open after 5 publish failures, try again after 10s
var kafkaBreaker = NewCircuitBreaker(5, 10*time.Second)

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
				L().Error("outbox_worker_error", zap.Error(err))
			}
		}
	}()
}

func processOutboxBatch() error {
	ctx := context.Background()

	// -----------------------------
	// Update pending gauge (best effort)
	// -----------------------------
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

	// -----------------------------
	// 1️⃣ Lock batch quickly
	// -----------------------------
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

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

	// Release row locks ASAP
	if err := tx.Commit(ctx); err != nil {
		return err
	}

	if len(batch) == 0 {
		return nil
	}

	// -----------------------------
	// 2️⃣ Publish outside transaction
	// -----------------------------
	for _, it := range batch {

		// ✅ Circuit breaker protects Kafka publish
		err := kafkaBreaker.Execute(func() error {
			return publishToKafka(it.id, []byte(it.payload))
		})

		// If breaker is OPEN, we do NOT increment attempts or DLQ.
		// We just back off briefly and let next tick retry.
		if errors.Is(err, ErrCircuitOpen) {
			L().Warn("kafka_breaker_open_skip_publish",
				zap.String("outbox_id", it.id),
				zap.String("breaker_state", string(kafkaBreaker.State())),
			)
			time.Sleep(500 * time.Millisecond)
			return nil // stop batch early (prevents busy looping)
		}

		if err != nil {
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
				  AND published_at IS NULL
				RETURNING attempts
			`, it.id, err.Error(), maxBackoffSecs).Scan(&attempts)

			if err2 != nil {
				L().Error("outbox_record_publish_failure_failed",
					zap.String("outbox_id", it.id),
					zap.Error(err2),
				)
				continue
			}

			if attempts >= maxAttempts {
				if err := moveToDLQ(ctx, it.id); err != nil {
					L().Error("outbox_move_to_dlq_failed",
						zap.String("outbox_id", it.id),
						zap.Error(err),
					)
				} else if outboxDLQTotal != nil {
					outboxDLQTotal.Inc()
				}
			}

			continue
		}

		// -----------------------------
		// 3️⃣ Success → mark published safely
		// -----------------------------
		res, err := db.Exec(ctx, `
			UPDATE outbox
			SET published_at = NOW(),
			    last_error = NULL
			WHERE id = $1
			  AND published_at IS NULL
		`, it.id)

		if err != nil {
			L().Error("outbox_mark_published_failed",
				zap.String("outbox_id", it.id),
				zap.Error(err),
			)
			continue
		}

		if res.RowsAffected() == 0 {
			// Another worker already handled it
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
	defer func() { _ = tx.Rollback(ctx) }()

	_, err = tx.Exec(ctx, `
		INSERT INTO outbox_dlq (
			id, aggregate_type, aggregate_id, event_type,
			payload, created_at, attempts, last_error
		)
		SELECT id, aggregate_type, aggregate_id, event_type,
		       payload, created_at, attempts, last_error
		FROM outbox
		WHERE id = $1
	`, id)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		DELETE FROM outbox
		WHERE id = $1
	`, id)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}
