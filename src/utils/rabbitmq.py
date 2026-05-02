import aio_pika
import asyncio
from logger_config import setup_logger

logger = setup_logger()

async def get_connection(host: str, user: str, password: str) -> aio_pika.RobustConnection:
    return await aio_pika.connect_robust(
        f"amqp://{user}:{password}@{host}/",
    )

async def publish_message(
    channel: aio_pika.Channel,
    queue_name: str,
    message: str,
):
    await channel.declare_queue(queue_name, durable=True)
    await channel.default_exchange.publish(
        aio_pika.Message(
            body=message.encode(),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        ),
        routing_key=queue_name,
    )
    logger.debug(f"Published message to {queue_name}")