import asyncio
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import aio_pika
from logger_config import setup_logger
from dotenv import load_dotenv

load_dotenv()
logger = setup_logger()

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_USER = os.getenv("RABBITMQ_USER")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS")
QUEUE_NAME = "flights"
RAW_PATH = Path("lakehouse") / "raw" / "flights"


async def drain_queue(channel: aio_pika.Channel):
    queue = await channel.declare_queue(QUEUE_NAME, durable=True)
    messages = []

    while True:
        try:
            message = await queue.get(timeout=2)
            async with message.process():
                payload = json.loads(message.body.decode())
                messages.append(payload)
        except aio_pika.exceptions.QueueEmpty:
            break

    logger.info(f"Drained {len(messages)} messages from '{QUEUE_NAME}' queue")
    return messages


def write_parquet(records: list[dict]):
    df = pd.DataFrame(records)

    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    timestamp_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    output_dir = RAW_PATH / f"date={date_str}"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"flights_{timestamp_str}.parquet"
    df.to_parquet(output_path, index=False, engine="pyarrow")

    logger.info(f"Wrote {len(df)} records to {output_path}")
    return output_path

async def drain_queue_with_connection():
    connection = await aio_pika.connect_robust(
        f"amqp://{RABBITMQ_USER}:{RABBITMQ_PASS}@{RABBITMQ_HOST}/"
    )
    async with connection:
        channel = await connection.channel()
        return await drain_queue(channel)

async def run():
    logger.info("Starting flights consumer")

    if not (records := await drain_queue_with_connection()):
        logger.info("No messages in queue — exiting")
        return

    write_parquet(records)
    logger.info("Flights consumer finished")

if __name__ == "__main__":
    asyncio.run(run())