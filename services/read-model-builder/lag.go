package main

import (
	"context"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

func StartLagReporter(
	brokersEnv string,
	topic string,
	group string,
	offsets *offsetTracker,
	interval time.Duration,
) {
	brokers := strings.Split(brokersEnv, ",")

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for range ticker.C {
			updateLagOnce(brokers, topic, group, offsets)
		}
	}()
}

func updateLagOnce(
	brokers []string,
	topic string,
	group string,
	offsets *offsetTracker,
) {
	snapshot := offsets.Snapshot()
	if len(snapshot) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	broker := strings.TrimSpace(brokers[0])

	for partition, committed := range snapshot {
		endOffset, err := fetchEndOffset(ctx, broker, topic, partition)
		if err != nil {
			log.Printf("lag fetch error topic=%s partition=%d err=%v", topic, partition, err)
			continue
		}

		// endOffset is next-to-be-written offset
		lag := endOffset - committed - 1
		if lag < 0 {
			lag = 0
		}

		kafkaPartitionLag.
			WithLabelValues(topic, strconv.Itoa(partition), group).
			Set(float64(lag))
	}
}

func fetchEndOffset(
	ctx context.Context,
	broker string,
	topic string,
	partition int,
) (int64, error) {

	conn, err := kafka.DialLeader(ctx, "tcp", broker, topic, partition)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	return conn.ReadLastOffset()
}