package main

import (
	"context"
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
		brokersEnv = "localhost:9092" // local fallback
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

func publishToKafka(eventKey string, payload []byte) error {
	if kafkaWriter == nil {
		return nil
	}

	msg := kafka.Message{
		Key:   []byte(eventKey),
		Value: payload,
		Time:  time.Now(),
		Headers: []kafka.Header{
			{Key: "event_type", Value: []byte(eventKey)},
			{Key: "version", Value: []byte("v1")},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return kafkaWriter.WriteMessages(ctx, msg)
}
