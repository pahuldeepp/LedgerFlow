import json
import asyncio
from aiokafka import AIOKafkaConsumer
from .db import get_conn
from .envelope import event_type, transaction_id, amount_from_event
from psycopg import errors  # Assuming you're using psycopg for DB interaction

async def run_consumer():
    # Initialize Kafka consumer
    consumer = AIOKafkaConsumer(
        settings.kafka_consume_topic,
        bootstrap_servers=[settings.kafka_brokers],
        group_id=settings.kafka_consumer_group,
        enable_auto_commit=False,  # Disable auto commit
        auto_offset_reset="earliest",  # Start from the earliest offset
        value_deserializer=lambda v: v,
        api_version="auto",
        metadata_max_age_ms=300000,
    )

    # Start consuming messages
    await consumer.start()
    print("Billing consumer started...")

    try:
        async for msg in consumer:
            try:
                # Deserialize the message value
                evt = json.loads(msg.value.decode("utf-8"))
            except Exception as e:
                print(f"Billing consumer: bad JSON, skipping message: {e}")
                # Commit the offset so the consumer doesn't try to reprocess this message
                await consumer.commit()
                continue

            # Optional event type filtering (if needed)
            et = event_type(evt)
            if et and et != TX_EVENT_NAME:
                await consumer.commit()
                continue

            try:
                # Extract transaction details
                tx_id = transaction_id(evt)
                amount = amount_from_event(evt)
            except Exception as e:
                print(f"Billing consumer: invalid event data, skipping: {e}")
                await consumer.commit()
                continue

            try:
                # Use the database connection
                with get_conn() as conn:
                    with conn.transaction():
                        # Insert transaction details into invoices table
                        cur = conn.cursor()
                        cur.execute(
                            """
                            INSERT INTO billing.invoices (transaction_id, amount)
                            VALUES (%s, %s)
                            ON CONFLICT (transaction_id) DO NOTHING
                            RETURNING invoice_id, status, created_at
                            """,
                            (tx_id, amount),
                        )

                        # Fetch the inserted row
                        row = cur.fetchone()

                        # If row is None, it means the transaction already exists (idempotent)
                        if row is None:
                            await consumer.commit()
                            continue

                        invoice_id = str(row["invoice_id"])

                        # Prepare the event for "invoice_created"
                        out_evt = {
                            "event_type": "invoice_created",
                            "data": {
                                "invoice_id": invoice_id,
                                "transaction_id": tx_id,
                                "amount": amount,
                                "status": "created",
                            },
                        }

                        # Insert the event into the outbox table
                        cur.execute(
                            """
                            INSERT INTO billing.outbox (event_type, payload)
                            VALUES (%s, %s::jsonb)
                            """,
                            ("invoice_created", json.dumps(out_evt)),
                        )

                # Commit Kafka offset ONLY after DB transaction succeeds
                await consumer.commit()

            except errors.Error as db_err:
                # Log DB error but do not commit, so Kafka will retry
                print(f"Billing consumer: DB error: {db_err}")
                await asyncio.sleep(1.0)  # Sleep before retrying

    finally:
        # Ensure consumer is stopped properly
        await consumer.stop()