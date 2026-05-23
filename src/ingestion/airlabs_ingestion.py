# src/ingestion/airlabs_ingestion.py
import asyncio
import httpx
import os
from datetime import datetime, timezone
from dotenv import load_dotenv
from logger_config import setup_logger
from utils.parquet_writer import write_parquet

load_dotenv()
logger = setup_logger()

API_KEY = os.getenv("AIRLABS_API_KEY")
BASE_URL = "https://airlabs.co/api/v9/schedules"

AIRPORTS = ["EWR", "ORD", "SFO", "JFK", "LAX", "ATL", "DFW"]


def enrich_flight(flight: dict, fetched_at: str) -> dict:
    return {
        **flight,
        "_fetched_at": fetched_at,
        "_source": "airlabs",
    }


async def fetch_flights(client: httpx.AsyncClient, iata: str, direction: str) -> list[dict]:
    param_key = "arr_iata" if direction == "arr" else "dep_iata"
    params = {"api_key": API_KEY, param_key: iata}
    try:
        response = await client.get(BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        if "error" in data:
            logger.error(f"API error for {iata} {direction}: {data['error']}")
            return []
        flights = data.get("response", [])
        logger.info(f"Fetched {len(flights)} {direction} flights for {iata}")
        return flights
    except httpx.HTTPError as e:
        logger.error(f"HTTP error fetching {direction} flights for {iata}: {e}")
        return []


async def run():
    logger.info("Starting AirLabs ingestion")
    fetched_at = datetime.now(timezone.utc).isoformat()
    all_records = []

    async with httpx.AsyncClient() as client:
        for airport in AIRPORTS:
            for direction in ["arr", "dep"]:
                flights = await fetch_flights(client, airport, direction)
                for flight in flights:
                    all_records.append(enrich_flight(flight, fetched_at))
                await asyncio.sleep(1)

    write_parquet(all_records, "flights")
    logger.info(f"AirLabs ingestion complete — {len(all_records)} records written")


if __name__ == "__main__":
    asyncio.run(run())