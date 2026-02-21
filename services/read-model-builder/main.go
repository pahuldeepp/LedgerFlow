package main

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	
	"log"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	
	"syscall"
	"time"
		"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)
type dbtx interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

const (
	workerCount   = 10
	jobBufferSize = 100
	// FIX: maxAttempts was duplicated — define it only here for the read-model-builder.
	maxAttempts = 5
)

type result struct {
	msg    kafka.Message
	ok     bool
	errMsg string
}

//
// ============================================================
// THREAD-SAFE COMMITTED OFFSETS (for lag monitor)
// ============================================================
//

type offsetTracker struct {
	mu      sync.RWMutex
	offsets map[int]int64
}

func newOffsetTracker() *offsetTracker {
	return &offsetTracker{offsets: make(map[int]int64)}
}

func (t *offsetTracker) Set(partition int, offset int64) {
	t.mu.Lock()
	t.offsets[partition] = offset
	t.mu.Unlock()
}

func (t *offsetTracker) Get(partition int) (int64, bool) {
	t.mu.RLock()
	v, ok := t.offsets[partition]
	t.mu.RUnlock()
	return v, ok
}

func (t *offsetTracker) Snapshot() map[int]int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	cp := make(map[int]int64, len(t.offsets))
	for k, v := range t.offsets {
		cp[k] = v
	}
	return cp
}

//
// ============================================================
// MAIN
// ============================================================
//

func main() {
	rand.Seed(time.Now().UnixNano())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	dsn := mustEnv("DATABASE_URL")
	kafkaBrokers := mustEnv("KAFKA_BROKERS")
	topic := mustEnv("KAFKA_TOPIC")

	// -------------------------
	// CONNECT DB
	// -------------------------
	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatal("db connection error:", err)
	}
	defer db.Close()
	log.Println("Connected to Postgres")

	// -------------------------
	// REBUILD MODE
	// -------------------------
	rebuild := strings.EqualFold(os.Getenv("REBUILD"), "true")

	consumerGroup := "read-model-builder"
	startOffset := kafka.LastOffset
	if rebuild {
		consumerGroup = "read-model-builder-rebuild-" + time.Now().Format("20060102-150405")
		startOffset = kafka.FirstOffset
		log.Println("REBUILD MODE ENABLED")

		if err := rebuildReadModel(ctx, db); err != nil {
			log.Fatal("rebuild failed:", err)
		}
	}

	brokers := strings.Split(kafkaBrokers, ",")

	startMetricsServer(db, brokers)

	// -------------------------
	// KAFKA READER
	// -------------------------
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		GroupID:     consumerGroup,
		StartOffset: startOffset,
	})
	defer reader.Close()
	log.Println("Kafka consumer started (group:", consumerGroup, ")")

	// -------------------------
	// DLQ WRITER
	// -------------------------
	dlqWriter := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic + ".dlq",
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll,
	}
	defer dlqWriter.Close()

	// -------------------------
	// PIPELINE CHANNELS
	// -------------------------
	jobs := make(chan kafka.Message, jobBufferSize)
	results := make(chan result, jobBufferSize)

	// -------------------------
	// WORKER POOL
	// -------------------------
	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go worker(ctx, db, jobs, results, i, &wg)
	}

	// -------------------------
	// COMMITTER
	// -------------------------
	offsets := newOffsetTracker()


	StartLagReporter(kafkaBrokers, topic, consumerGroup, offsets, 5*time.Second)

	committerDone := make(chan struct{})
	go committer(ctx, db, reader, dlqWriter, results, committerDone, offsets)

	// -------------------------
	// FETCH LOOP
	// -------------------------
	fetchDone := make(chan struct{})
	go func() {
		defer close(fetchDone)
		for {
			msg, err := reader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Println("fetch error:", err)
				time.Sleep(2 * time.Second)
				continue
			}
			select {
			case jobs <- msg:
			case <-ctx.Done():
				return
			}
		}
	}()

	// -------------------------
	// SHUTDOWN
	// -------------------------
	<-sigChan
	log.Println("Shutdown signal received")
		
	cancel()
	<-fetchDone

	close(jobs)
	wg.Wait()

	close(results)
	<-committerDone

	log.Println("Graceful shutdown complete")
}

//
// ============================================================
// WORKER
// ============================================================
//

func worker(
	ctx context.Context,
	db *pgxpool.Pool,
	jobs <-chan kafka.Message,
	results chan<- result,
	id int,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	log.Println("Worker started:", id)

	for msg := range jobs {
		ok, errMsg := processMessage(ctx, db, msg)

		select {
		case results <- result{msg: msg, ok: ok, errMsg: errMsg}:
		case <-ctx.Done():
			return
		}
	}
}

func processMessage(ctx context.Context, db *pgxpool.Pool, msg kafka.Message) (bool, string) {
	start := time.Now()

	var event TransactionPostedEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		eventsFailed.Inc()
		eventProcessingDuration.Observe(time.Since(start).Seconds())
		log.Printf("poison message at partition=%d offset=%d: %v", msg.Partition, msg.Offset, err)
		return true, "" // poison pill -> commit to skip
	}

	log.Printf("processing event_id=%s transaction_id=%s", event.EventID, event.TransactionID)

	processed, err := handleTransactionPosted(ctx, db, event)
	if err != nil {
		eventsFailed.Inc()
		eventProcessingDuration.Observe(time.Since(start).Seconds())
		log.Printf("handler error event_id=%s: %v", event.EventID, err)
		return false, err.Error()
	}

	if processed {
		eventsProcessed.Inc()
		log.Printf("processed event_id=%s", event.EventID)
	} else {
		eventsDuplicates.Inc()
		log.Printf("duplicate skipped event_id=%s", event.EventID)
	}

	eventProcessingDuration.Observe(time.Since(start).Seconds())
	return true, ""
}

//
// ============================================================
// COMMITTER
// ============================================================
//

func committer(
	ctx context.Context,
	db *pgxpool.Pool,
	reader *kafka.Reader,
	dlqWriter *kafka.Writer,
	results <-chan result,
	done chan<- struct{},
	offsets *offsetTracker,
) {
	defer close(done)

	nextOffset := make(map[int]int64)
	pending := make(map[int]map[int64]result)

	for r := range results {
		p := r.msg.Partition
		off := r.msg.Offset

		if pending[p] == nil {
			pending[p] = make(map[int64]result)
		}
		pending[p][off] = r

		if _, ok := nextOffset[p]; !ok {
			nextOffset[p] = off
		}

		for {
			want := nextOffset[p]
			item, ok := pending[p][want]
			if !ok {
				break
			}

			if !item.ok {
				var ev TransactionPostedEvent
				if err := json.Unmarshal(item.msg.Value, &ev); err != nil || ev.EventID == "" {
					_ = dlqWriter.WriteMessages(ctx, kafka.Message{
						Key:   item.msg.Key,
						Value: item.msg.Value,
						Headers: append(item.msg.Headers,
							kafka.Header{Key: "dlq-error", Value: []byte("unparseable payload")},
						),
					})
					if err := reader.CommitMessages(ctx, item.msg); err != nil {
						log.Println("commit after poison DLQ failed:", err)
						break
					}

					offsets.Set(p, want)
					delete(pending[p], want)
					nextOffset[p]++
					continue
				}

				retryCount, nextRetryAt, exists, err := getFailure(ctx, db, ev.EventID)
				if err != nil {
					log.Println("getFailure error:", err)
					break
				}

				if exists && time.Now().Before(nextRetryAt) {
					log.Printf("Backoff active: event_id=%s retry=%d next=%s",
						ev.EventID, retryCount, nextRetryAt.Format(time.RFC3339))
					break
				}

				newRetry := 1
				if exists {
					newRetry = retryCount + 1
				}

				if newRetry >= maxAttempts {
					log.Printf("DLQ: event_id=%s attempts=%d err=%s", ev.EventID, newRetry, item.errMsg)

					_ = dlqWriter.WriteMessages(ctx, kafka.Message{
						Key:   item.msg.Key,
						Value: item.msg.Value,
						Headers: append(item.msg.Headers,
							kafka.Header{Key: "dlq-error", Value: []byte(item.errMsg)},
							kafka.Header{Key: "dlq-attempts", Value: []byte(intToBytes(newRetry))},
							kafka.Header{Key: "source-topic", Value: []byte(item.msg.Topic)},
							kafka.Header{Key: "source-partition", Value: []byte(intToBytes(item.msg.Partition))},
							kafka.Header{Key: "source-offset", Value: []byte(int64ToBytes(item.msg.Offset))},
						),
					})

					if err := reader.CommitMessages(ctx, item.msg); err != nil {
						log.Println("commit after DLQ failed:", err)
						break
					}

					// FIX: don't swallow errors
					if err := clearFailure(ctx, db, ev.EventID); err != nil {
						log.Println("clearFailure error:", err)
					}

					offsets.Set(p, want)
					delete(pending[p], want)
					nextOffset[p]++
					continue
				}

				backoff := computeBackoff(newRetry)
				next := time.Now().Add(backoff)

				// FIX: don't swallow errors
				if err := updateFailure(ctx, db, ev.EventID, newRetry, next, item.errMsg); err != nil {
					log.Println("updateFailure error:", err)
				}

				log.Printf("Retry scheduled: event_id=%s retry=%d next=%s",
					ev.EventID, newRetry, next.Format(time.RFC3339))

				break
			}

			if err := reader.CommitMessages(ctx, item.msg); err != nil {
				log.Println("commit error:", err)
				break
			}

			var ev TransactionPostedEvent
			if err := json.Unmarshal(item.msg.Value, &ev); err == nil && ev.EventID != "" {
				// FIX: don't swallow errors
				if err := clearFailure(ctx, db, ev.EventID); err != nil {
					log.Println("clearFailure error:", err)
				}
			}

			offsets.Set(p, want)
			delete(pending[p], want)
			nextOffset[p]++
		}
	}
}

//
// ============================================================
// FAILURE STORAGE (durable retries)
// Ensure this table exists in read_model schema:
//   CREATE TABLE read_model.consumer_failures (...)
// ============================================================
//

func getFailure(ctx context.Context, db *pgxpool.Pool, eventID string) (retryCount int, nextRetry time.Time, ok bool, err error) {
	row := db.QueryRow(ctx, `
		SELECT retry_count, next_retry_at
		FROM read_model.consumer_failures
		WHERE event_id = $1
	`, eventID)

	if scanErr := row.Scan(&retryCount, &nextRetry); scanErr != nil {
		if errors.Is(scanErr, pgx.ErrNoRows) {
			return 0, time.Time{}, false, nil
		}
		return 0, time.Time{}, false, scanErr
	}
	return retryCount, nextRetry, true, nil
}

func updateFailure(ctx context.Context, db *pgxpool.Pool, eventID string, retryCount int, nextRetry time.Time, lastError string) error {
	_, err := db.Exec(ctx, `
		INSERT INTO read_model.consumer_failures
			(event_id, retry_count, next_retry_at, last_error, updated_at)
		VALUES
			($1, $2, $3, $4, now())
		ON CONFLICT (event_id)
		DO UPDATE SET
			retry_count   = EXCLUDED.retry_count,
			next_retry_at = EXCLUDED.next_retry_at,
			last_error    = EXCLUDED.last_error,
			updated_at    = now()
	`, eventID, retryCount, nextRetry, lastError)
	return err
}

func clearFailure(ctx context.Context, db *pgxpool.Pool, eventID string) error {
	_, err := db.Exec(ctx, `DELETE FROM read_model.consumer_failures WHERE event_id = $1`, eventID)
	return err
}

//
// ============================================================
// BACKOFF
// ============================================================
//

func computeBackoff(retryCount int) time.Duration {
	base := 1 * time.Second
	max := 30 * time.Second

	exp := math.Pow(2, float64(retryCount-1))
	delay := time.Duration(float64(base) * exp)
	if delay > max {
		delay = max
	}

	jitter := time.Duration(rand.Intn(250)) * time.Millisecond
	return delay + jitter
}

//
// ============================================================
// REBUILD READ MODEL (FINAL FIX)
// - truncates everything including failures + processed_events
// - restart identity for clean rebuild
// ============================================================
//

func rebuildReadModel(ctx context.Context, db *pgxpool.Pool) error {
	_, err := db.Exec(ctx, `
		TRUNCATE TABLE
			read_model.account_balances,
			read_model.tx_feed,
			read_model.account_tx_index,
			read_model.processed_events,
			read_model.consumer_failures
		RESTART IDENTITY;
	`)
	return err
}

//
// ============================================================
// ATOMIC PROJECTION + IDEMPOTENCY (FINAL FIX)
// ============================================================
//


//
// ============================================================
// UTIL
// ============================================================
//

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("%s not set", key)
	}
	return v
}

func intToBytes(n int) []byte {
	if n == 0 {
		return []byte("0")
	}
	buf := make([]byte, 0, 12)
	x := n
	if x < 0 {
		buf = append(buf, '-')
		x = -x
	}
	tmp := make([]byte, 0, 10)
	for x > 0 {
		tmp = append(tmp, byte('0'+(x%10)))
		x /= 10
	}
	for i := len(tmp) - 1; i >= 0; i-- {
		buf = append(buf, tmp[i])
	}
	return buf
}

func int64ToBytes(n int64) []byte {
	if n == 0 {
		return []byte("0")
	}
	buf := make([]byte, 0, 24)
	x := n
	if x < 0 {
		buf = append(buf, '-')
		x = -x
	}
	tmp := make([]byte, 0, 20)
	for x > 0 {
		tmp = append(tmp, byte('0'+(x%10)))
		x /= 10
	}
	for i := len(tmp) - 1; i >= 0; i-- {
		buf = append(buf, tmp[i])
	}
	return buf
}