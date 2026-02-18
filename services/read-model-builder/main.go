package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

const workerCount = 10
const jobBufferSize = 100

type result struct {
	msg      kafka.Message
	commitOK bool // true only if processing succeeded OR we chose to skip (bad payload)
}

func main() {
	startMetricsServer()

	// Cancelable context so FetchMessage + CommitMessages can be stopped on shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// OS signals for graceful shutdown (Ctrl+C / docker stop / k8s SIGTERM)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("DATABASE_URL not set")
	}

	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		log.Fatal("KAFKA_BROKERS not set")
	}

	topic := os.Getenv("KAFKA_TOPIC")
	if topic == "" {
		log.Fatal("KAFKA_TOPIC not set")
	}

	// Connect to Postgres
	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatal("db connection error:", err)
	}
	defer db.Close()
	log.Println("Connected to Postgres")

	// Kafka reader
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: strings.Split(kafkaBrokers, ","),
		Topic:   topic,
		GroupID: "read-model-builder",
	})
	defer reader.Close()
	log.Println("Kafka consumer started, waiting for messages...")

	// Buffered job channel (backpressure control)
	jobs := make(chan kafka.Message, jobBufferSize)

	// Results channel (workers publish processing outcome here)
	results := make(chan result, jobBufferSize)

	// Start worker pool (drainable via WaitGroup + close(jobs))
	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go worker(ctx, db, jobs, results, i, &wg)
	}

	// Single committer: commit offsets IN ORDER per partition
	committerDone := make(chan struct{})
	go committer(ctx, reader, results, committerDone)

	// Fetch loop runs in its own goroutine so main can orchestrate shutdown
	fetchDone := make(chan struct{})
	go func() {
		defer close(fetchDone)

		for {
			msg, err := reader.FetchMessage(ctx)
			if err != nil {
				// If we're shutting down, FetchMessage will return error because ctx is canceled
				if ctx.Err() != nil {
					return
				}
				log.Println("fetch error:", err)
				time.Sleep(2 * time.Second)
				continue
			}

			// Backpressure: blocks when buffer full
			select {
			case jobs <- msg:
			case <-ctx.Done():
				return
			}
		}
	}()

	// Wait for shutdown signal
	<-sigChan
	log.Println("Shutdown signal received")

	// Stop fetch loop, then drain workers, then stop committer
	cancel()
	<-fetchDone

	close(jobs)
	wg.Wait()

	close(results)
	<-committerDone

	log.Println("Graceful shutdown complete")
}

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
		ok := processMessage(ctx, db, msg)
		// If ctx canceled during shutdown, best-effort exit
		select {
		case results <- result{msg: msg, commitOK: ok}:
		case <-ctx.Done():
			return
		}
	}
}

func processMessage(
	ctx context.Context,
	db *pgxpool.Pool,
	msg kafka.Message,
) bool {
	start := time.Now()

	var event TransactionPostedEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		log.Println("invalid payload:", err)

		eventsFailed.Inc()
		eventProcessingDuration.Observe(time.Since(start).Seconds())

		return true // commit to skip poison message
	}

	processed, err := handleTransactionPosted(ctx, db, event)
	if err != nil {
		log.Println("handler error:", err)

		eventsFailed.Inc()
		eventProcessingDuration.Observe(time.Since(start).Seconds())

		return false // do NOT commit => retry
	}

	if processed {
		eventsProcessed.Inc()
		log.Println("Processed event:", event.EventID)
	} else {
		eventsDuplicates.Inc()
		log.Println("Duplicate skipped:", event.EventID)
	}

	eventProcessingDuration.Observe(time.Since(start).Seconds())
	return true
}

func committer(ctx context.Context, reader *kafka.Reader, results <-chan result, done chan<- struct{}) {
	defer close(done)

	// next expected offset per partition
	nextOffset := make(map[int]int64)
	// pending results per partition keyed by offset
	pending := make(map[int]map[int64]result)

	for r := range results {
		p := r.msg.Partition

		if pending[p] == nil {
			pending[p] = make(map[int64]result)
		}
		pending[p][r.msg.Offset] = r

		// Initialize nextOffset to the first seen offset (for this runtime).
		// Note: after a restart, the group offset determines where FetchMessage starts,
		// so "first seen" is safe as the start-of-stream for this process.
		if _, ok := nextOffset[p]; !ok {
			nextOffset[p] = r.msg.Offset
		}

		// Commit sequentially as far as possible
		for {
			want := nextOffset[p]
			item, ok := pending[p][want]
			if !ok {
				break
			}

			if !item.commitOK {
				// processing failed -> stop committing further offsets for this partition
				break
			}

			if err := reader.CommitMessages(ctx, item.msg); err != nil {
				// If shutting down, exit; otherwise keep pending so we can retry commits
				if ctx.Err() != nil {
					return
				}
				log.Println("commit error:", err)
				// Don't advance nextOffset; leave item pending to retry later
				break
			}

			delete(pending[p], want)
			nextOffset[p] = want + 1
		}
	}
}
