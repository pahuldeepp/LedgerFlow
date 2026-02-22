package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	adminv1 "ledger-go/proto"
)

var (
	db           *pgxpool.Pool
	kafkaBrokers = os.Getenv("KAFKA_BROKERS") // e.g. "localhost:9092,localhost:9093"
)

type adminServer struct {
	adminv1.UnimplementedAdminServiceServer
}

func newAdminServer() *adminServer {
	return &adminServer{}
}

func init() {
	// Initialize the database connection pool
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		fmt.Println("DATABASE_URL not set")
		os.Exit(1)
	}

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		fmt.Println("Unable to parse DATABASE_URL:", err)
		os.Exit(1)
	}

	cfg.MaxConns = 10
	cfg.MinConns = 2
	cfg.MaxConnLifetime = time.Hour
	cfg.MaxConnIdleTime = 30 * time.Minute

	db, err = pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		fmt.Println("Unable to connect to database:", err)
		os.Exit(1)
	}
	fmt.Println("Database connected")
}

func (s *adminServer) ListOutbox(ctx context.Context, req *adminv1.ListOutboxRequest) (*adminv1.ListOutboxResponse, error) {
	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = 50 // default value
	}

	// Use the global db pool for query execution
	rows, err := db.Query(ctx, `
		SELECT id::text, event_type, created_at, attempts,
		       COALESCE(published_at::text, ''), COALESCE(last_error, '')
		FROM outbox
		ORDER BY created_at DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("list_outbox query failed: %w", err)
	}
	defer rows.Close()

	var items []*adminv1.OutboxItem
	for rows.Next() {
		var (
			id          string
			eventType   string
			createdAt   time.Time
			attempts    int32
			publishedAt string
			lastError   string
		)
		if err := rows.Scan(&id, &eventType, &createdAt, &attempts, &publishedAt, &lastError); err != nil {
			return nil, fmt.Errorf("list_outbox scan failed: %w", err)
		}
		items = append(items, &adminv1.OutboxItem{
			Id:          id,
			EventType:   eventType,
			CreatedAt:   createdAt.UTC().Format(time.RFC3339Nano),
			Attempts:    attempts,
			PublishedAt: publishedAt,
			LastError:   lastError,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list_outbox rows error: %w", err)
	}

	return &adminv1.ListOutboxResponse{Items: items}, nil
}

func (s *adminServer) GetOutbox(ctx context.Context, req *adminv1.GetOutboxRequest) (*adminv1.GetOutboxResponse, error) {
	var (
		id          string
		eventType   string
		createdAt   time.Time
		attempts    int32
		publishedAt string
		lastError   string
	)

	err := db.QueryRow(ctx, `
		SELECT id::text, event_type, created_at, attempts,
		       COALESCE(published_at::text, ''), COALESCE(last_error, '')
		FROM outbox
		WHERE id = $1
	`, req.GetId()).Scan(&id, &eventType, &createdAt, &attempts, &publishedAt, &lastError)
	if err != nil {
		return nil, fmt.Errorf("get_outbox query failed: %w", err)
	}

	return &adminv1.GetOutboxResponse{
		Item: &adminv1.OutboxItem{
			Id:          id,
			EventType:   eventType,
			CreatedAt:   createdAt.UTC().Format(time.RFC3339Nano),
			Attempts:    attempts,
			PublishedAt: publishedAt,
			LastError:   lastError,
		},
	}, nil
}

func (s *adminServer) ListDlq(ctx context.Context, req *adminv1.ListDlqRequest) (*adminv1.ListDlqResponse, error) {
	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = 50 // default value
	}

	// DLQ = items that have been attempted but not yet published
	rows, err := db.Query(ctx, `
		SELECT id::text, event_type, COALESCE(aggregate_id::text, ''),
		       created_at, COALESCE(published_at::text, ''),
		       attempts, COALESCE(last_error, '')
		FROM outbox
		WHERE published_at IS NULL AND attempts > 0
		ORDER BY attempts DESC, created_at DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("list_dlq query failed: %w", err)
	}
	defer rows.Close()

	var items []*adminv1.DlqItem
	for rows.Next() {
		var (
			id          string
			eventType   string
			aggregateId string
			createdAt   time.Time
			dlqAt       string
			attempts    int32
			lastError   string
		)
		if err := rows.Scan(&id, &eventType, &aggregateId, &createdAt, &dlqAt, &attempts, &lastError); err != nil {
			return nil, fmt.Errorf("list_dlq scan failed: %w", err)
		}
		items = append(items, &adminv1.DlqItem{
			Id:          id,
			EventType:   eventType,
			AggregateId: aggregateId,
			CreatedAt:   createdAt.UTC().Format(time.RFC3339Nano),
			DlqAt:       dlqAt,
			Attempts:    attempts,
			LastError:   lastError,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list_dlq rows error: %w", err)
	}

	return &adminv1.ListDlqResponse{Items: items}, nil
}