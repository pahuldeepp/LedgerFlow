package main

import (
	"context"
	"errors"
	"os"
	"time"

	"go.uber.org/zap"
)

const (
    maxAttempts    = 5
    maxBackoffSecs = 60
    batchSize      = 100
    pollInterval   = 200 * time.Millisecond
    claimTTL       = 30 * time.Second
)

// Circuit breaker: open after 5 publish failures, try again after 10s
var kafkaBreaker = NewCircuitBreaker(5, 10*time.Second)

type outboxItem struct {
	id        string
	eventType string
	payload   string
}

func workerID() string {
	if v := os.Getenv("WORKER_ID"); v != "" {
		return v
	}
	// ok for local dev; in k8s set WORKER_ID to pod name
	return "worker-1"
}

func startOutboxWorker() {
    workerCount := 4 // run 4 concurrent workers

    for i := 0; i < workerCount; i++ {
        go func(workerID int) {
            ticker := time.NewTicker(pollInterval)
            defer ticker.Stop()

            for range ticker.C {
                if err := processOutboxBatch(); err != nil {
                    L().Error("outbox_worker_error",
                        zap.Int("worker", workerID),
                        zap.Error(err),
                    )
                }
            }
        }(i)
    }
}

func processOutboxBatch() error {
	ctx := context.Background()
	wid := workerID()

	// -----------------------------
	// Update pending gauge
	// -----------------------------
	var cnt int64
	_ = db.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM outbox
		WHERE published_at IS NULL
		  AND attempts < $1
		  AND (next_attempt_at IS NULL OR next_attempt_at <= NOW())
	`, maxAttempts).Scan(&cnt)

	OutboxPending.Set(float64(cnt))

	// -----------------------------
	// 1️⃣ CLAIM batch atomically (fixes duplicate publish)
	// -----------------------------
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Claim rows by setting locked_by/locked_at INSIDE the transaction
	// Only claimed rows are returned and published.
	rows, err := tx.Query(ctx, `
		WITH picked AS (
			SELECT id
			FROM outbox
			WHERE published_at IS NULL
			  AND attempts < $1
			  AND (next_attempt_at IS NULL OR next_attempt_at <= NOW())
			  AND (
				 locked_by IS NULL
				 OR locked_at < NOW() - $3::interval
			  )
			ORDER BY created_at ASC
			FOR UPDATE SKIP LOCKED
			LIMIT $2
		)
		UPDATE outbox o
		SET locked_by = $4,
			locked_at = NOW()
		FROM picked
		WHERE o.id = picked.id
		RETURNING o.id::text, o.event_type, o.payload::text
	`, maxAttempts, batchSize, claimTTL.String(), wid)
	if err != nil {
		return err
	}
	defer rows.Close()

	var batch []outboxItem
	for rows.Next() {
		var it outboxItem
		if err := rows.Scan(&it.id, &it.eventType, &it.payload); err != nil {
			return err
		}
		batch = append(batch, it)
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}

	if len(batch) == 0 {
		return nil
	}

	// -----------------------------
	// 2️⃣ Publish claimed rows + mark published guarded by locked_by
	// -----------------------------
	for _, it := range batch {

		err := kafkaBreaker.Execute(func() error {
			return publishToKafka(it.id, it.eventType, []byte(it.payload))
		})

		if errors.Is(err, ErrCircuitOpen) {
			L().Warn("kafka_breaker_open_skip_publish",
				zap.String("outbox_id", it.id),
				zap.String("breaker_state", string(kafkaBreaker.State())),
			)
			time.Sleep(500 * time.Millisecond)
			return nil
		}

		if err != nil {
			OutboxPublishFailedTotal.Inc()

			// Increment attempts + compute backoff atomically
			var attempts int
			err2 := db.QueryRow(ctx, `
				UPDATE outbox
				SET attempts = attempts + 1,
				    last_error = $2,
				    next_attempt_at = NOW() + make_interval(
				        secs => LEAST(POWER(2, attempts + 1)::int, $3)
				    )
				WHERE id = $1
				  AND published_at IS NULL
				  AND locked_by = $4
				RETURNING attempts
			`, it.id, err.Error(), maxBackoffSecs, wid).Scan(&attempts)

			if err2 != nil {
				L().Error("outbox_record_publish_failure_failed",
					zap.String("outbox_id", it.id),
					zap.Error(err2),
				)
				continue
			}

			// unlock so it can be retried later (or reclaimed)
			_, _ = db.Exec(ctx, `
				UPDATE outbox
				SET locked_by = NULL,
					locked_at = NULL
				WHERE id = $1 AND locked_by = $2 AND published_at IS NULL
			`, it.id, wid)

			if attempts >= maxAttempts {
				if err := moveToDLQ(ctx, it.id); err != nil {
					L().Error("outbox_move_to_dlq_failed",
						zap.String("outbox_id", it.id),
						zap.Error(err),
					)
				} else {
					OutboxDLQTotal.Inc()
				}
			}

			continue
		}

		// -----------------------------
		// 3️⃣ Mark published (guarded) + release lock
		// -----------------------------
		res, err := db.Exec(ctx, `
			UPDATE outbox
			SET published_at = NOW(),
			    last_error = NULL,
			    locked_by = NULL,
			    locked_at = NULL
			WHERE id = $1
			  AND published_at IS NULL
			  AND locked_by = $2
		`, it.id, wid)

		if err != nil {
			L().Error("outbox_mark_published_failed",
				zap.String("outbox_id", it.id),
				zap.Error(err),
			)
			OutboxPublishFailedTotal.Inc()
			continue
		}

		if res.RowsAffected() == 0 {
			// This should be rare now; means lock was reclaimed or published already
			L().Warn("outbox_mark_published_noop",
				zap.String("outbox_id", it.id),
			)
			continue
		}

		OutboxPublishedTotal.Inc()
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
