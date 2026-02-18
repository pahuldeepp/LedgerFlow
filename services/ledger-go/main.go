package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var db *pgxpool.Pool

// ==========================
// STRUCTS
// ==========================

type Entry struct {
	AccountID string `json:"account_id"`
	Direction string `json:"direction"` // debit | credit
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
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("DATABASE_URL not set")
	}

	var err error
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		log.Fatalf("Unable to parse DATABASE_URL: %v", err)
	}

	// pool tuning (good defaults; adjust later based on load)
	config.MaxConns = 10
	config.MinConns = 2
	config.MaxConnLifetime = time.Hour
	config.MaxConnIdleTime = 30 * time.Minute

	db, err = pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		log.Fatalf("Unable to connect to database: %v", err)
	}
	log.Println("Connected to database successfully")

	// Kafka + outbox
	initKafkaProducer()
	startOutboxWorker()

	// Router
	r := gin.New()
	r.Use(gin.Logger(), gin.Recovery())

	// Hard request timeout (prevents hung requests)
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
			// entry validation (must be strict in a ledger)
			if e.AccountID == "" || e.Amount <= 0 {
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid entry (account_id required, amount must be > 0)"})
				return
			}

			switch e.Direction {
			case "debit":
				debitTotal += e.Amount
			case "credit":
				creditTotal += e.Amount
			default:
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid direction (must be debit or credit)"})
				return
			}
		}

		if debitTotal != creditTotal {
			c.JSON(http.StatusBadRequest, gin.H{"error": "debits must equal credits"})
			return
		}

		ctx := c.Request.Context()

		tx, err := db.Begin(ctx)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to start db tx"})
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

		// Idempotency via unique idempotency_key + "xmax=0" trick for inserted row
		err = tx.QueryRow(ctx, `
			INSERT INTO public.transactions (idempotency_key, status)
			VALUES ($1, $2)
			ON CONFLICT (idempotency_key)
			DO UPDATE SET idempotency_key = EXCLUDED.idempotency_key
			RETURNING id, (xmax = 0) AS is_new
		`, req.IdempotencyKey, "posted").Scan(&txID, &isNew)

		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		if !isNew {
			c.JSON(http.StatusOK, gin.H{
				"status":         "already_processed",
				"transaction_id": txID,
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
				c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to insert entry"})
				return
			}
		}

		// Event identity separation
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
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to marshal event"})
			return
		}

		// Transactional outbox insert
		_, err = tx.Exec(ctx, `
			INSERT INTO public.outbox (aggregate_type, aggregate_id, event_type, payload)
			VALUES ($1, $2, $3, $4)
		`, "transaction", txID, "ledger.transaction.posted", payloadBytes)

		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to insert into outbox"})
			return
		}

		if err := tx.Commit(ctx); err != nil {
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
	// BALANCE (READ MODEL — TRUE CQRS)
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
			// If not projected yet OR missing, return 0
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
	// SERVER + GRACEFUL SHUTDOWN
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

	// shutdown on SIGINT/SIGTERM
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		log.Printf("Server running on port %s", port)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("listen error: %v", err)
		}
	}()

	<-stop
	log.Println("Shutting down...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_ = srv.Shutdown(ctx)

	// Clean up infra
	closeKafkaProducer()
	db.Close()

	log.Println("Shutdown complete")
}
