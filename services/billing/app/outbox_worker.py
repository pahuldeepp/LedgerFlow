import asyncio
import json
from aiokafka import AIOKafkaProducer
from psycopg import errors

from .settings import settings
from .db import get_conn

BATCH_SIZE = 50

async def run_outbox_worker():
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_brokers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    await producer.start()

    try:
        while True:
            did_work = False

            try:
                with get_conn() as conn:
                    with conn.transaction():
                        cur = conn.cursor()
                        cur.execute(
                            """
                            WITH cte AS (
                              SELECT id
                              FROM billing.outbox
                              WHERE published_at IS NULL
                                AND next_attempt_at <= now()
                              ORDER BY created_at
                              FOR UPDATE SKIP LOCKED
                              LIMIT %s
                            )
                            SELECT o.id, o.event_type, o.payload
                            FROM billing.outbox o
                            JOIN cte ON cte.id = o.id
                            """,
                            (BATCH_SIZE,),
                        )
                        rows = cur.fetchall()

                        if not rows:
                            # no rows
                            pass
                        else:
                            for r in rows:
                                outbox_id = str(r["id"])
                                et = r["event_type"]
                                payload = r["payload"]

                                try:
                                    await producer.send_and_wait(
                                        settings.kafka_produce_topic,
                                        payload,
                                        key=outbox_id.encode("utf-8"),
                                        headers=[("event_type", et.encode("utf-8"))],
                                    )

                                    cur.execute(
                                        "UPDATE billing.outbox SET published_at = now() WHERE id = %s",
                                        (outbox_id,),
                                    )
                                    did_work = True

                                except Exception as e:
                                    cur.execute(
                                        """
                                        UPDATE billing.outbox
                                        SET attempts = attempts + 1,
                                            last_error = %s,
                                            next_attempt_at = now()
                                              + (interval '1 second' * LEAST(60, (2 ^ LEAST(attempts, 6))))
                                        WHERE id = %s
                                        """,
                                        (str(e), outbox_id),
                                    )

            except errors.Error as db_err:
                print("Billing outbox worker: DB error:", db_err)

            await asyncio.sleep(0.2 if did_work else 1.0)

    finally:
        await producer.stop()