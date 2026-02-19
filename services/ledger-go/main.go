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

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

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

	// Kafka + outbox
	initKafkaProducer()
	startOutboxWorker()

	// ==========================
	// ROUTER
	// ==========================

	r := gin.New()
	r.Use(RequestID(), ZapLogger(), ZapRecovery())

	// Hard request timeout
	r.Use(func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
		defer cancel()
		c.Request = c.Request.WithContext(ctx)
		c.Next()
	})

	// ==========================
	// HEALTH
	// ==========================

	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	// ==========================
	// METRICS
	// ==========================

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

		// 🔒 SERIALIZABLE isolation
		tx, err := db.BeginTx(ctx, pgx.TxOptions{
			IsoLevel: pgx.Serializable,
		})
		if err != nil {
			L().Error("db_tx_begin_failed", zap.Error(err))
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to start tx"})
			return
		}

		committed := false
		defer func() {
			if !committed {
				_ = tx.Rollback(ctx)
			}
		}()

		var txID string
		var isNew bool

		err = tx.QueryRow(ctx, `
			INSERT INTO public.transactions (idempotency_key, status)
			VALUES ($1, $2)
			ON CONFLICT (idempotency_key)
			DO UPDATE SET idempotency_key = EXCLUDED.idempotency_key
			RETURNING id, (xmax = 0) AS is_new
		`, req.IdempotencyKey, "posted").Scan(&txID, &isNew)

		if err != nil {
			L().Error("tx_insert_failed", zap.Error(err))
			c.JSON(http.StatusInternalServerError, gin.H{"error": "insert failed"})
			return
		}

		if !isNew {
			c.JSON(http.StatusOK, gin.H{
				"status":         "already_processed",
				"transaction_id": txID,
			})
			return
		}

		for _, e := range req.Entries {
			_, err := tx.Exec(ctx, `
				INSERT INTO public.entries (transaction_id, account_id, direction, amount)
				VALUES ($1, $2, $3, $4)
			`, txID, e.AccountID, e.Direction, e.Amount)

			if err != nil {
				L().Error("entry_insert_failed", zap.Error(err))
				c.JSON(http.StatusInternalServerError, gin.H{"error": "entry insert failed"})
				return
			}
		}

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
			L().Error("event_marshal_failed", zap.Error(err))
			c.JSON(http.StatusInternalServerError, gin.H{"error": "event marshal failed"})
			return
		}

		_, err = tx.Exec(ctx, `
			INSERT INTO public.outbox (aggregate_type, aggregate_id, event_type, payload)
			VALUES ($1, $2, $3, $4)
		`, "transaction", txID, "ledger.transaction.posted", payloadBytes)

		if err != nil {
			L().Error("outbox_insert_failed", zap.Error(err))
			c.JSON(http.StatusInternalServerError, gin.H{"error": "outbox insert failed"})
			return
		}

		if err := tx.Commit(ctx); err != nil {
			L().Error("tx_commit_failed", zap.Error(err))
			c.JSON(http.StatusInternalServerError, gin.H{"error": "commit failed"})
			return
		}
		committed = true

		c.JSON(http.StatusCreated, gin.H{
			"transaction_id": txID,
			"debit_total":    debitTotal,
			"credit_total":   creditTotal,
		})
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
