package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)


func main() {
	ctx := context.Background()

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("DATABASE_URL not set")
	}

	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		log.Fatal("KAFKA_BROKERS not set")
	}

	topic := os.Getenv("KAFKA_TOPIC")
	if topic == "" {
		log.Fatal("KAFKA_TOPIC not set")
	}

	// Connect to Postgres
	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatal("db connection error:", err)
	}
	defer db.Close()

	log.Println("Connected to Postgres")

	// Create Kafka reader
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBrokers},
		Topic:   topic,
		GroupID: "read-model-builder",
	})
	defer reader.Close()

	log.Println("Kafka consumer started, waiting for messages...")

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			log.Println("fetch error:", err)
			time.Sleep(2 * time.Second)
			continue
		}

		var event TransactionPostedEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Println("invalid payload:", err)
			_ = reader.CommitMessages(ctx, msg)
			continue
		}

		log.Println("Processing event:", event.EventID)

		if err := handleTransactionPosted(ctx, db, event); err != nil {
			log.Println("handler error:", err)
			// Do NOT commit — Kafka will redeliver
			continue
		}

		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Println("commit error:", err)
		}
	}
}
