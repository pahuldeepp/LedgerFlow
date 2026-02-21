package main

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// =========================
	// HTTP Metrics
	// =========================

	HTTPDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "Duration of HTTP requests.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "path", "status"},
	)

	HTTPRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total HTTP requests.",
		},
		[]string{"method", "path", "status"},
	)

	// =========================
	// Outbox Metrics
	// =========================

	OutboxPending = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "outbox_pending_total",
			Help: "Number of unpublished outbox records.",
		},
	)

	OutboxPublishedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "outbox_published_total",
			Help: "Total successfully published outbox events.",
		},
	)

	OutboxPublishFailedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "outbox_publish_failed_total",
			Help: "Total failed publish attempts.",
		},
	)

	OutboxDLQTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "outbox_dlq_total",
			Help: "Total events moved to DLQ.",
		},
	)

	// =========================
	// Kafka Circuit Breaker
	// =========================

	KafkaCircuitBreakerState = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "kafka_circuit_breaker_state",
			Help: "Kafka circuit breaker state (0=closed,1=open,2=half_open).",
		},
	)

	KafkaCircuitBreakerOpenTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "kafka_circuit_breaker_open_total",
			Help: "Total number of times Kafka circuit breaker opened.",
		},
	)
)

// Optional helper
func ObserveHTTPRequest(method, path, status string, duration time.Duration) {
	HTTPDuration.
		WithLabelValues(method, path, status).
		Observe(duration.Seconds())

	HTTPRequestsTotal.
		WithLabelValues(method, path, status).
		Inc()
}
