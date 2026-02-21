package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

var kafkaWriter *kafka.Writer
var kafkaTopic string

func initKafkaProducer() {
	brokersEnv := os.Getenv("KAFKA_BROKERS")
	if brokersEnv == "" {
		brokersEnv = "localhost:9092"
	}

	kafkaTopic = os.Getenv("KAFKA_TOPIC")
	if kafkaTopic == "" {
		kafkaTopic = "ledger.events"
	}

	brokers := strings.Split(brokersEnv, ",")

	kafkaWriter = &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        kafkaTopic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		Compression:  kafka.Snappy,
		Async:        false,
		BatchTimeout: 50 * time.Millisecond,
		WriteTimeout: 5 * time.Second,
		ReadTimeout:  5 * time.Second,
	}

	log.Println("Kafka producer initialized")
	log.Println("Brokers:", brokers)
	log.Println("Topic:", kafkaTopic)
}

func closeKafkaProducer() {
	if kafkaWriter != nil {
		log.Println("Closing Kafka producer...")
		_ = kafkaWriter.Close()
	}
}

// FIXED: event_type header must be the real event type, not the outbox id.
func publishToKafka(eventKey string, eventType string, payload []byte) error {
	if kafkaWriter == nil {
		return fmt.Errorf("kafka writer not initialized")
	}

	msg := kafka.Message{
		Key:   []byte(eventKey),
		Value: payload,
		Time:  time.Now(),
		Headers: []kafka.Header{
			{Key: "event_id", Value: []byte(eventKey)},
			{Key: "event_type", Value: []byte(eventType)},
			{Key: "version", Value: []byte("v1")},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return kafkaWriter.WriteMessages(ctx, msg)
}