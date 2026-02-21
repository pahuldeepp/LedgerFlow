package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	eventsProcessed = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "events_processed_total",
			Help: "Total number of successfully processed events",
		},
	)

	eventsDuplicates = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "events_duplicates_total",
			Help: "Total number of duplicate events detected",
		},
	)

	eventsFailed = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "events_failed_total",
			Help: "Total number of failed event processings",
		},
	)

	eventProcessingDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "event_processing_duration_seconds",
			Help:    "Time taken to process an event",
			Buckets: prometheus.DefBuckets,
		},
	)

	kafkaPartitionLag = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "kafka_partition_lag",
		Help: "Current Kafka consumer lag per partition",
	},
	[]string{"topic", "partition", "group"},
)
)

func init() {
prometheus.MustRegister(
	eventsProcessed,
	eventsDuplicates,
	eventsFailed,
	eventProcessingDuration,
	kafkaPartitionLag,
)

}

func startMetricsServer(db *pgxpool.Pool, brokers []string) {
	addr := os.Getenv("METRICS_ADDR")
	if addr == "" {
		addr = ":9101"
	}

	mux := http.NewServeMux()

	// Prometheus endpoint
	mux.Handle("/metrics", promhttp.Handler())

	// Real health endpoint
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {

		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()

		// -------------------
		// DB Check
		// -------------------
		if err := db.Ping(ctx); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("db unhealthy\n"))
			return
		}

		// -------------------
		// Kafka TCP Check
		// -------------------
		if len(brokers) == 0 {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("no brokers configured\n"))
			return
		}

		b := strings.TrimSpace(brokers[0])
		conn, err := net.DialTimeout("tcp", b, 2*time.Second)
		if err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("kafka unreachable\n"))
			return
		}
		_ = conn.Close()

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok\n"))
	})

	go func() {
		log.Println("Metrics server running on", addr)
		if err := http.ListenAndServe(addr, mux); err != nil {
			log.Fatalf("metrics server error: %v", err)
		}
	}()
}
