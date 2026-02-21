import asyncio
from .consumer import run_consumer
from .outbox_worker import run_outbox_worker     

async def main():
    print("Billing service starting...")
    await asyncio.gather(
        run_consumer(),
        run_outbox_worker(),
    )

if __name__ == "__main__":
    asyncio.run(main())