package main


import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)
type TransactionPostedEvent struct {
	EventID       string  `json:"event_id"`
	TransactionID string  `json:"transaction_id"`
	Entries       []Entry `json:"entries"`
	Status        string  `json:"status"`
	OccurredAt    string  `json:"occurred_at"`
	Version       int     `json:"version"`
}

var db *pgxpool.Pool

type Entry struct {
	AccountID string `json:"account_id" binding:"required"`
	Direction string `json:"direction" binding:"required"` // debit | credit
	Amount    int64  `json:"amount" binding:"required"`
}

type CreateTransactionRequest struct {
	IdempotencyKey string  `json:"idempotency_key" binding:"required"`
	Entries        []Entry `json:"entries" binding:"required"`
}

func main() {
    dsn := "postgres://postgres:postgres@127.0.0.1:5432/ledgerflow?sslmode=disable"

    var err error
    db, err = pgxpool.New(context.Background(), dsn)
    if err != nil {
        log.Fatalf("Unable to connect to database: %v", err)
    }
    defer db.Close()

    log.Println("Connected to database successfully")

    // 🔥 Start Kafka + Outbox worker
    initKafkaProducer()
    startOutboxWorker()
    defer closeKafkaProducer()

    r := gin.New()
    r.Use(gin.Logger(), gin.Recovery())

	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status": "ok",
		})
	})

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
		tx, err := db.Begin(ctx)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to start db tx"})
			return
		}
		defer tx.Rollback(ctx)

		var txID string
		var isNew bool

		err = tx.QueryRow(ctx, `
			INSERT INTO transactions (idempotency_key, status)
			VALUES ($1, $2)
			ON CONFLICT (idempotency_key)
			DO UPDATE SET idempotency_key = EXCLUDED.idempotency_key
			RETURNING id, (xmax = 0) AS is_new
		`, req.IdempotencyKey, "posted").Scan(&txID, &isNew)

		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		// If already processed, return same transaction ID
		if !isNew {
			c.JSON(http.StatusOK, gin.H{
				"status":         "already_processed",
				"transaction_id": txID,
			})
			return
		}

		// Insert entries only if new
		for _, e := range req.Entries {
			_, err := tx.Exec(ctx, `
				INSERT INTO entries (transaction_id, account_id, direction, amount)
				VALUES ($1, $2, $3, $4)
			`, txID, e.AccountID, e.Direction, e.Amount)

			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to insert entry"})
				return
			}
		}

		event := TransactionPostedEvent{
	EventID:       txID, // (better: separate UUID for event_id later)
	TransactionID: txID,
	Entries:       req.Entries,
	Status:        "posted",
	OccurredAt:    time.Now().UTC().Format(time.RFC3339Nano),
	Version:       1,
}

payloadBytes, err := json.Marshal(event)
if err != nil {
	c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to marshal event"})
	return
}

	_, err = tx.Exec(ctx, `
	INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload)
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

		c.JSON(http.StatusCreated, gin.H{
			"transaction_id": txID,
			"debit_total":    debitTotal,
			"credit_total":   creditTotal,
		})
	})

	// ==========================
	// METRICS ENDPOINT
	// ==========================
	r.GET("/metrics", func(c *gin.Context) {
		promhttp.Handler().ServeHTTP(c.Writer, c.Request)
	})

	// ==========================
	// BALANCE ENDPOINT
	// ==========================
	r.GET("/accounts/:id/balance", func(c *gin.Context) {
		accountID := c.Param("id")

		var balance int64

		err := db.QueryRow(context.Background(), `
			SELECT COALESCE(SUM(
				CASE
					WHEN direction = 'credit' THEN amount
					WHEN direction = 'debit'  THEN -amount
				END
			), 0)
			FROM entries
			WHERE account_id = $1
		`, accountID).Scan(&balance)

		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"account_id": accountID,
			"balance":    balance,
		})
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8081"
	}



	log.Printf("Server running on port %s", port)
	log.Fatal(r.Run(":" + port))
}
