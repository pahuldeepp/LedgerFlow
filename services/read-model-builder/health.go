package main

import (
	"context"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

)

type HealthDeps struct {
	DB      *pgxpool.Pool
	Brokers []string
}

func (h HealthDeps) Check(ctx context.Context) error {
	// DB check
	dbCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	if err := h.DB.Ping(dbCtx); err != nil {
		return err
	}
	// Kafka check (TCP dial to one broker)
	// brokers are like "host:9092"
	if len(h.Brokers) == 0 {
		return context.DeadlineExceeded
	}
	
	b := strings.TrimSpace(h.Brokers[0])
	dialer := &net.Dialer{Timeout: 2 * time.Second}

	conn, err := dialer.DialContext(ctx, "tcp", b)
	if err != nil {
		return err
	}
	_ = conn.Close()

	return nil

}

func HealthHandler(deps HealthDeps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()

		if err := deps.Check(ctx); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("unhealthy\n"))
			return
		}

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	}
}



