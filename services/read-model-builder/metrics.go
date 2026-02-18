package main

import (
	"log"
	"net/http"
	"os"

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

	kafkaLagGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "kafka_lag_gauge",
			Help: "Current Kafka consumer lag",
		},
	)
)

func init() {
	prometheus.MustRegister(
		eventsProcessed,
		eventsDuplicates,
		eventsFailed,
		eventProcessingDuration,
		kafkaLagGauge,
	)
}

func startMetricsServer() {
	addr := os.Getenv("METRICS_ADDR")
	if addr == "" {
		addr = ":9101"
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	go func() {
		log.Println("Metrics server running on", addr)
		if err := http.ListenAndServe(addr, mux); err != nil {
			log.Fatalf("metrics server error: %v", err)
		}
	}()
}
