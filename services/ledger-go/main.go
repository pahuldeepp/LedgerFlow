import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

var (
	kafkaBrokers = os.Getenv("KAFKA_BROKERS") // e.g. "localhost:9092,localhost:9093"
	db           *pgxpool.Pool
)

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

func main() {
	// 1) Logger first
	initLogger()
	defer logSync()

	// 2) Env
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		L().Fatal("DATABASE_URL not set")
	}
	if kafkaBrokers == "" {
		L().Warn("KAFKA_BROKERS not set (health check will report kafka_unreachable)")
	}

	// 3) DB pool config (set before creating pool)
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		L().Fatal("Unable to parse DATABASE_URL", zap.Error(err))
	}
	cfg.MaxConns = 10
	cfg.MinConns = 2
	cfg.MaxConnLifetime = time.Hour
	cfg.MaxConnIdleTime = 30 * time.Minute

	db, err = pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		L().Fatal("Unable to connect to database", zap.Error(err))
	}
	defer db.Close()
	L().Info("database_connected")

	// 4) gRPC server (if you have one)
	grpcSrv, grpcLis, err := startGRPCServer(":9090")
	if err != nil {
		L().Fatal("grpc_listen_error", zap.Error(err))
	}
	defer func() {
		L().Info("grpc_shutdown")
		grpcSrv.GracefulStop()
		_ = grpcLis.Close()
	}()

	// 5) Kafka + outbox
	initKafkaProducer()
	defer closeKafkaProducer()
	startOutboxWorker()

	// 6) Gin router
	r := gin.New()
	r.Use(RequestID(), ZapLogger(), ZapRecovery())

	// Hard request timeout (global)
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

		// DB ping
		if err := db.Ping(ctx); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "db_unhealthy", "error": err.Error()})
			return
		}

		// Kafka reachability: pick first broker from CSV list
		broker := firstBroker(kafkaBrokers)
		if broker == "" {
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "kafka_unset"})
			return
		}

		conn, err := net.DialTimeout("tcp", broker, 1*time.Second)
		if err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "kafka_unreachable", "error": err.Error()})
			return
		}
		_ = conn.Close()

		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	// Metrics
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))
	// Admin - Outbox Viewer
	r.GET("/admin/outbox", RequireAnyGroup("admin"), func(c *gin.Context) {
		ctx := c.Request.Context()

		rows, err := db.Query(ctx, `
			SELECT id::text, event_type, payload::text, created_at, published_at, attempts, last_error
			FROM outbox
			ORDER BY created_at DESC
			LIMIT 50
		`)
		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
		defer rows.Close()

		type item struct {
			ID          string     `json:"id"`
			EventType   string     `json:"event_type"`
			Payload     string     `json:"payload"`
			CreatedAt   time.Time  `json:"created_at"`
			PublishedAt *time.Time `json:"published_at"`
			Attempts    int        `json:"attempts"`
			LastError   *string    `json:"last_error"`
		}

		out := []item{}
		for rows.Next() {
			var it item
			if err := rows.Scan(&it.ID, &it.EventType, &it.Payload, &it.CreatedAt, &it.PublishedAt, &it.Attempts, &it.LastError); err != nil {
				c.JSON(500, gin.H{"error": err.Error()})
				return
			}
			out = append(out, it)
		}

		c.JSON(200, out)
	})

	// Create Transaction
	r.POST("/transactions", RequireAnyGroup("service", "admin"), func(c *gin.Context) {
		var req CreateTransactionRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		if len(req.Entries) < 2 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "at least 2 entries required"})
			return
		}

		var debitTotal, creditTotal int64
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
			tx, err := db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
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
				_ = tx.Rollback(ctx)
				c.JSON(http.StatusInternalServerError, gin.H{"error": "insert failed"})
				return
			}

			// already processed
			if errors.Is(err, pgx.ErrNoRows) {
				var existingID string
				if lookupErr := tx.QueryRow(ctx,
					`SELECT id FROM public.transactions WHERE idempotency_key = $1`,
					req.IdempotencyKey,
				).Scan(&existingID); lookupErr != nil {
					_ = tx.Rollback(ctx)
					c.JSON(http.StatusInternalServerError, gin.H{"error": "lookup failed"})
					return
				}

				_ = tx.Rollback(ctx)
				c.JSON(http.StatusOK, gin.H{
					"status":         "already_processed",
					"transaction_id": existingID,
				})
				return
			}

			// entries
			for _, e := range req.Entries {
				_, err := tx.Exec(ctx, `
					INSERT INTO public.entries (transaction_id, account_id, direction, amount)
					VALUES ($1, $2, $3, $4)
				`, txID, e.AccountID, e.Direction, e.Amount)
				if err != nil {
					_ = tx.Rollback(ctx)
					L().Error("entry_insert_failed", zap.Error(err))
					c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
					return
				}
			}

			// authoritative invariant check
			var dbDebitTotal, dbCreditTotal int64
			err = tx.QueryRow(ctx, `
				SELECT
					COALESCE(SUM(CASE WHEN direction = 'debit' THEN amount ELSE 0 END), 0),
					COALESCE(SUM(CASE WHEN direction = 'credit' THEN amount ELSE 0 END), 0)
				FROM public.entries
				WHERE transaction_id = $1
			`, txID).Scan(&dbDebitTotal, &dbCreditTotal)
			if err != nil {
				_ = tx.Rollback(ctx)
				c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to verify ledger invariant"})
				return
			}
			if dbDebitTotal != dbCreditTotal {
				_ = tx.Rollback(ctx)
				c.JSON(http.StatusBadRequest, gin.H{
					"error": fmt.Sprintf("ledger invariant violation: debits (%d) != credits (%d)", dbDebitTotal, dbCreditTotal),
				})
				return
			}

			// outbox event
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
				_ = tx.Rollback(ctx)
				c.JSON(http.StatusInternalServerError, gin.H{"error": "event marshal failed"})
				return
			}

			_, err = tx.Exec(ctx, `
				INSERT INTO public.outbox (aggregate_type, aggregate_id, event_type, payload)
				VALUES ($1, $2, $3, $4)
			`, "transaction", txID, "ledger.transaction.posted", payloadBytes)
			if err != nil {
				_ = tx.Rollback(ctx)
				c.JSON(http.StatusInternalServerError, gin.H{"error": "outbox insert failed"})
				return
			}

			// commit + retry on serialization failure
			if commitErr := tx.Commit(ctx); commitErr != nil {
				if pgErr, ok := commitErr.(*pgconn.PgError); ok && pgErr.Code == "40001" {
					continue
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

	// Admin - DLQ Viewer
	r.GET("/admin/dlq", RequireAnyGroup("admin"), func(c *gin.Context) {
		// Fetch DLQ records (replace with actual DB query logic)
		dlqRecords, err := getDLQRecordsFromDB()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to fetch DLQ records", "details": err.Error()})
			return
		}

		// Return the DLQ records in the JSON response
		c.JSON(http.StatusOK, gin.H{
			"dlq_records": dlqRecords,
		})
	})

	// Balance
	r.GET("/accounts/:id/balance", RequireAnyGroup("viewer", "service", "admin"), func(c *gin.Context) {
		accountID := c.Param("id")

		var balance int64
		err := db.QueryRow(c.Request.Context(), `
			SELECT balance
			FROM read_model.account_balances
			WHERE account_id = $1
		`, accountID).Scan(&balance)

		if err != nil {
			c.JSON(http.StatusOK, gin.H{"account_id": accountID, "balance": 0})
			return
		}
		c.JSON(http.StatusOK, gin.H{"account_id": accountID, "balance": balance})
	})

	// 7) HTTP server + graceful shutdown
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

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_ = srv.Shutdown(shutdownCtx)
	L().Info("shutdown_complete")
}

func getDLQRecordsFromDB() ([]map[string]interface{}, error) {
	// Fetch DLQ records from the database
	rows, err := db.Query(context.Background(), "SELECT id, event_type, payload, failure_reason, moved_at, attempts FROM outbox_dlq")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var dlqRecords []map[string]interface{}
	for rows.Next() {
		var id, eventType, payload, failureReason, movedAt string
		var attempts int
		if err := rows.Scan(&id, &eventType, &payload, &failureReason, &movedAt, &attempts); err != nil {
			return nil, err
		}

		// Add the record to the list
		dlqRecord := map[string]interface{}{
			"id":             id,
			"event_type":     eventType,
			"payload":        payload,
			"failure_reason": failureReason,
			"moved_at":       movedAt,
			"attempts":       attempts,
		}
		dlqRecords = append(dlqRecords, dlqRecord)
	}

	// Return the fetched DLQ records
	return dlqRecords, nil
}

func firstBroker(csv string) string {
	csv = strings.TrimSpace(csv)
	if csv == "" {
		return ""
	}
	parts := strings.Split(csv, ",")
	if len(parts) == 0 {
		return ""
	}
	return strings.TrimSpace(parts[0])
}