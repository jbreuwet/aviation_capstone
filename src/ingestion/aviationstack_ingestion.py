import asyncio
import httpx
import json
import os
from datetime import datetime, timezone
from logger_config import setup_logger
from dotenv import load_dotenv
from utils.parquet_writer import write_parquet

load_dotenv()
logger = setup_logger()

API_KEY = os.getenv("AVIATIONSTACK_API_KEY")
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

async def fetch_flights(client: httpx.AsyncClient, iata: str, direction: str):
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


def enrich_flight(flight: dict, fetched_at: str):
    return {
        **flight,
        "_fetched_at": fetched_at,
        "_source": "aviationstack",
    }

async def run():
    logger.info("Starting AviationStack producer")
    fetched_at = datetime.now(timezone.utc).isoformat()
    all_records = []

    async with httpx.AsyncClient() as client:
        for airport in AIRPORTS:
            for direction in ["arr", "dep"]:
                flights = await fetch_flights(client, airport, direction)

                for flight in flights:
                    enriched = enrich_flight(flight, fetched_at)
                    all_records.append(enriched)
                    
                await asyncio.sleep(1)

    write_parquet(all_records, "flights")
    logger.info(f"AviationStack producer finished, {len(all_records)} records written.")


if __name__ == "__main__":
    asyncio.run(run())