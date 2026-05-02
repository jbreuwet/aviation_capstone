import asyncio
import httpx
import json
import os
from datetime import datetime, timezone
from logger_config import setup_logger
from dotenv import load_dotenv
from utils.rabbitmq import get_connection, publish_message

load_dotenv()
logger = setup_logger()

API_KEY = os.getenv("AVIATIONSTACK_API_KEY")
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_USER = os.getenv("RABBITMQ_USER")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS")

QUEUE_NAME = "flights"
BASE_URL = "http://api.aviationstack.com/v1/flights"

AIRPORTS = [
    "EWR",  # Newark
    "ORD",  # Chicago O'Hare
    "SFO",  # San Francisco
    "JFK",  # New York JFK
    "LAX",  # Los Angeles
    "ATL",  # Atlanta
    "DFW",  # Dallas Fort Worth
]

async def fetch_flights(client: httpx.AsyncClient, iata: str, direction: str) -> list[dict]:
    """
    direction: 'arr' for arrivals, 'dep' for departures
    """
    param_key = "arr_iata" if direction == "arr" else "dep_iata"
    params = {
        "access_key": API_KEY,
        param_key: iata,
        "limit": 100,
    }

    try:
        response = await client.get(BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        if "error" in data:
            logger.error(f"API error for {iata} {direction}: {data['error']}")
            return []

        flights = data.get("data", [])
        logger.info(f"Fetched {len(flights)} {direction} flights for {iata}")
        return flights

    except httpx.HTTPError as e:
        logger.error(f"HTTP error fetching {direction} flights for {iata}: {e}")
        return []


def enrich_flight(flight: dict, fetched_at: str) -> dict:
    """Add pipeline metadata to each flight record."""
    return {
        **flight,
        "_fetched_at": fetched_at,
        "_source": "aviationstack",
    }


async def run():
    logger.info("Starting AviationStack producer")
    fetched_at = datetime.now(timezone.utc).isoformat()

    connection = await get_connection(RABBITMQ_HOST, RABBITMQ_USER, RABBITMQ_PASS)
    async with connection:
        channel = await connection.channel()

        async with httpx.AsyncClient() as client:
            for airport in AIRPORTS:
                for direction in ["arr", "dep"]:
                    flights = await fetch_flights(client, airport, direction)

                    for flight in flights:
                        enriched = enrich_flight(flight, fetched_at)
                        await publish_message(
                            channel,
                            QUEUE_NAME,
                            json.dumps(enriched),
                        )

                    # Be respectful to the API — small delay between calls
                    await asyncio.sleep(1)

    logger.info("AviationStack producer finished")


if __name__ == "__main__":
    asyncio.run(run())