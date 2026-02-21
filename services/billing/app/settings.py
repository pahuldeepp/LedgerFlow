from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    database_url: str
    kafka_brokers: str

    kafka_consume_topic: str = "ledger.transactions"
    kafka_produce_topic: str = "billing.events"
    kafka_consumer_group: str = "billing-service"

settings = Settings()