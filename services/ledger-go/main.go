package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
	"fmt"
	"net"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"

)
var kafkaBroker = os.Getenv("KAFKA_BROKERS")
var db *pgxpool.Pool

// ==========================
// STRUCTS
// ==========================

type Entry struct {
	AccountID string `json:"account_id"`
	Direction string `json:"direction"`
	Amount    int64  `json:"amount"`
}

type TransactionPostedEvent struct {
	EventID       string    `json:"event_id"`
	TransactionID string    `json:"transaction_id"`
	Entries       []Entry   `json:"entries"`
	Status        string    `json:"status"`
	OccurredAt    time.Time `json:"occurred_at"`
	Version       int       `json:"version"`
}

type CreateTransactionRequest struct {
	IdempotencyKey string  `json:"idempotency_key" binding:"required"`
	Entries        []Entry `json:"entries" binding:"required"`
}

// ==========================
// MAIN
// ==========================

func main() {
	initLogger()
	defer logSync()

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		L().Fatal("DATABASE_URL not set")
	}

	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		L().Fatal("Unable to parse DATABASE_URL", zap.Error(err))
	}

	config.MaxConns = 10
	config.MinConns = 2
	config.MaxConnLifetime = time.Hour
	config.MaxConnIdleTime = 30 * time.Minute

	db, err = pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		L().Fatal("Unable to connect to database", zap.Error(err))
	}
	L().Info("connected_to_database")

	initKafkaProducer()
	startOutboxWorker()

	r := gin.New()
	r.Use(RequestID(), ZapLogger(), ZapRecovery())

	// Hard request timeout
	r.Use(func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
		defer cancel()
		c.Request = c.Request.WithContext(ctx)
		c.Next()
	})

	// Health
	r.GET("/health", func(c *gin.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Check DB
	if err := db.Ping(ctx); err != nil {
		c.JSON(503, gin.H{"status": "db_unhealthy", "error": err.Error()})
		return
	}

	// Check Kafka (simple TCP dial)
	conn, err := net.DialTimeout("tcp", kafkaBroker, 1*time.Second)
	if err != nil {
		c.JSON(503, gin.H{"status": "kafka_unreachable", "error": err.Error()})
		return
	}
	conn.Close()

	c.JSON(200, gin.H{"status": "ok"})
})
	// Metrics
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))

	// ==========================
	// CREATE TRANSACTION
	// ==========================

	r.POST("/transactions", func(c *gin.Context) {
		var req CreateTransactionRequest

		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		if len(req.Entries) < 2 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "at least 2 entries required"})
			return
		}

		var debitTotal int64
		var creditTotal int64

		for _, e := range req.Entries {
			if e.AccountID == "" || e.Amount <= 0 {
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid entry"})
				return
			}

			switch e.Direction {
			case "debit":
				debitTotal += e.Amount
			case "credit":
				creditTotal += e.Amount
			default:
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid direction"})
				return
			}
		}

		if debitTotal != creditTotal {
			c.JSON(http.StatusBadRequest, gin.H{"error": "debits must equal credits"})
			return
		}

		ctx := c.Request.Context()
		const maxRetries = 3

		for attempt := 1; attempt <= maxRetries; attempt++ {

			tx, err := db.BeginTx(ctx, pgx.TxOptions{
				IsoLevel: pgx.Serializable,
			})
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to start tx"})
				return
			}

			var txID string

			err = tx.QueryRow(ctx, `
				INSERT INTO public.transactions (idempotency_key, status)
				VALUES ($1, $2)
				ON CONFLICT (idempotency_key) DO NOTHING
				RETURNING id
			`, req.IdempotencyKey, "posted").Scan(&txID)

			if err != nil && !errors.Is(err, pgx.ErrNoRows) {
				tx.Rollback(ctx)
				c.JSON(http.StatusInternalServerError, gin.H{"error": "insert failed"})
				return
			}

			// Already processed
			if errors.Is(err, pgx.ErrNoRows) {
				var existingID string
				if lookupErr := tx.QueryRow(ctx,
					`SELECT id FROM public.transactions WHERE idempotency_key = $1`,
					req.IdempotencyKey,
				).Scan(&existingID); lookupErr != nil {
					tx.Rollback(ctx)
					c.JSON(http.StatusInternalServerError, gin.H{"error": "lookup failed"})
					return
				}

				tx.Rollback(ctx)
				c.JSON(http.StatusOK, gin.H{
					"status":         "already_processed",
					"transaction_id": existingID,
				})
				return
			}

			// Insert entries
			for _, e := range req.Entries {
				_, err := tx.Exec(ctx, `
					INSERT INTO public.entries (transaction_id, account_id, direction, amount)
					VALUES ($1, $2, $3, $4)
				`, txID, e.AccountID, e.Direction, e.Amount)

				if err != nil {
    tx.Rollback(ctx)
    L().Error("entry_insert_failed", zap.Error(err))
    c.JSON(http.StatusInternalServerError, gin.H{
        "error": err.Error(),
    })
    return
}
			}
			// 🔒 Authoritative DB invariant check (debit == credit)
var dbDebitTotal int64
var dbCreditTotal int64

err = tx.QueryRow(ctx, `
    SELECT
        COALESCE(SUM(CASE WHEN direction = 'debit' THEN amount ELSE 0 END), 0),
        COALESCE(SUM(CASE WHEN direction = 'credit' THEN amount ELSE 0 END), 0)
    FROM public.entries
    WHERE transaction_id = $1
`, txID).Scan(&dbDebitTotal, &dbCreditTotal)

if err != nil {
    tx.Rollback(ctx)
    c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to verify ledger invariant"})
    return
}

if dbDebitTotal != dbCreditTotal {
    tx.Rollback(ctx)
    c.JSON(http.StatusBadRequest, gin.H{
        "error": fmt.Sprintf(
            "ledger invariant violation: debits (%d) != credits (%d)",
            dbDebitTotal, dbCreditTotal,
        ),
    })
    return
}

			// Create outbox event
			event := TransactionPostedEvent{
				EventID:       uuid.New().String(),
				TransactionID: txID,
				Entries:       req.Entries,
				Status:        "posted",
				OccurredAt:    time.Now().UTC(),
				Version:       1,
			}

			payloadBytes, err := json.Marshal(event)
			if err != nil {
				tx.Rollback(ctx)
				c.JSON(http.StatusInternalServerError, gin.H{"error": "event marshal failed"})
				return
			}

			_, err = tx.Exec(ctx, `
				INSERT INTO public.outbox (aggregate_type, aggregate_id, event_type, payload)
				VALUES ($1, $2, $3, $4)
			`, "transaction", txID, "ledger.transaction.posted", payloadBytes)

			if err != nil {
				tx.Rollback(ctx)
				c.JSON(http.StatusInternalServerError, gin.H{"error": "outbox insert failed"})
				return
			}

			// Commit
			if commitErr := tx.Commit(ctx); commitErr != nil {
				if pgErr, ok := commitErr.(*pgconn.PgError); ok && pgErr.Code == "40001" {
					continue // retry
				}
				c.JSON(http.StatusInternalServerError, gin.H{"error": "commit failed"})
				return
			}

			c.JSON(http.StatusCreated, gin.H{
				"transaction_id": txID,
				"debit_total":    debitTotal,
				"credit_total":   creditTotal,
			})
			return
		}

		c.JSON(http.StatusInternalServerError, gin.H{"error": "transaction failed after retries"})
	})

	// ==========================
	// BALANCE
	// ==========================

	r.GET("/accounts/:id/balance", func(c *gin.Context) {
		accountID := c.Param("id")

		var balance int64
		err := db.QueryRow(c.Request.Context(), `
			SELECT balance
			FROM read_model.account_balances
			WHERE account_id = $1
		`, accountID).Scan(&balance)

		if err != nil {
			c.JSON(http.StatusOK, gin.H{
				"account_id": accountID,
				"balance":    0,
			})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"account_id": accountID,
			"balance":    balance,
		})
	})

	// ==========================
	// SERVER
	// ==========================

	port := os.Getenv("PORT")
	if port == "" {
		port = "8081"
	}

	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           r,
		ReadHeaderTimeout: 5 * time.Second,
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		L().Info("server_started", zap.String("port", port))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			L().Fatal("listen_error", zap.Error(err))
		}
	}()

	<-stop
	L().Info("shutdown_start")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_ = srv.Shutdown(ctx)
	closeKafkaProducer()
	db.Close()

	L().Info("shutdown_complete")
}