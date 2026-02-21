import asyncio
import json
from aiokafka import AIOKafkaConsumer
from psycopg import errors

from .settings import settings
from .db import get_conn
from .envelope import event_type, transaction_id, amount_from_event

TX_EVENT_NAME = "ledger.transaction.posted"  # change if your ledger uses another name
async def run_consumer():
    consumer = AIOKafkaConsumer(
        settings.kafka_consume_topic,
        bootstrap_servers=[settings.kafka_brokers],
        group_id=settings.kafka_consumer_group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: v,
        api_version="auto",
        metadata_max_age_ms=300000,
    )

    await consumer.start()
    print("Billing consumer started...")

    try:
        async for msg in consumer:
            try:
                evt = json.loads(msg.value.decode("utf-8"))
            except Exception as e:
                print("Billing consumer: bad json, skipping:", e)
                await consumer.commit()
                continue

            # Optional event type filtering
            et = event_type(evt)
            if et and et != TX_EVENT_NAME:
                await consumer.commit()
                continue

            try:
                tx_id = transaction_id(evt)
                amount = amount_from_event(evt)
            except Exception as e:
                print("Billing consumer: invalid event, skipping:", e)
                await consumer.commit()
                continue

            try:
                with get_conn() as conn:
                    with conn.transaction():
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

                        row = cur.fetchone()

                        # Idempotent case
                        if row is None:
                            await consumer.commit()
                            continue

                        invoice_id = str(row["invoice_id"])

                        out_evt = {
                            "event_type": "invoice_created",
                            "data": {
                                "invoice_id": invoice_id,
                                "transaction_id": tx_id,
                                "amount": amount,
                                "status": "created",
                            },
                        }

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
                # Do NOT commit -> Kafka will retry
                print("Billing consumer: DB error:", db_err)
                await asyncio.sleep(1.0)

    finally:
        await consumer.stop()