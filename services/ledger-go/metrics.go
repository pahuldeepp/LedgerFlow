package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	outboxPending = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "outbox_pending_total",
			Help: "Number of unpublished outbox records ready for processing",
		},
	)

	outboxPublishedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "outbox_published_total",
			Help: "Total successfully published outbox events",
		},
	)

	outboxPublishFailedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "outbox_publish_failed_total",
			Help: "Total failed publish attempts",
		},
	)

	outboxDLQTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "outbox_dlq_total",
			Help: "Total events moved to DLQ",
		},
	)
)
