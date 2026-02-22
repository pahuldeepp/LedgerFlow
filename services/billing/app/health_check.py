# health_check.py
from fastapi import FastAPI
from psycopg import connect, OperationalError
from aiokafka import AIOKafkaProducer
from .settings import settings
import asyncio

app = FastAPI()

# Function to check database connectivity
async def check_database():
    try:
        # Example PostgreSQL connection using psycopg
        conn = await connect(dsn=settings.database_url)
        await conn.close()
        return True
    except OperationalError as e:
        print("Database error:", e)
        return False

# Function to check Kafka connectivity
async def check_kafka():
    try:
        producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_brokers)
        await producer.start()
        await producer.stop()
        return True
    except Exception as e:
        print("Kafka connection error:", e)
        return False

@app.get("/health")
async def health_check():
    # Perform checks for database and Kafka
    db_status = await check_database()
    kafka_status = await check_kafka()

    if db_status and kafka_status:
        return {"status": "healthy", "service": "Billing Service", "db_status": "up", "kafka_status": "up"}
    else:
        return {"status": "unhealthy", "service": "Billing Service", "db_status": "down" if not db_status else "up", "kafka_status": "down" if not kafka_status else "up"}