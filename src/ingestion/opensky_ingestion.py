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

BASE_URL = "https://opensky-network.org/api/states/all"
HEADERS = {"User-Agent": "aviation-capstone-project/1.0"}
POLL_INTERVAL = int(os.getenv("OPENSKY_POLL_INTERVAL", 60))

# Bounding box for continental US
BOUNDING_BOX = {
    "lamin": 25.0,
    "lomin": -125.0,
    "lamax": 50.0,
    "lomax": -65.0,
}

STATE_VECTOR_FIELDS = [
    "icao24",
    "callsign",
    "origin_country",
    "time_position",
    "last_contact",
    "longitude",
    "latitude",
    "baro_altitude",
    "on_ground",
    "velocity",
    "true_track",
    "vertical_rate",
    "sensors",
    "geo_altitude",
    "squawk",
    "spi",
    "position_source",
]

def enrich_position(state_vector: list, fetched_at: str):
    return {
        **dict(zip(STATE_VECTOR_FIELDS, state_vector)),
        "_fetched_at": fetched_at,
        "_source": "opensky",
    }

async def fetch_positions(client: httpx.AsyncClient):
    try:
        response = await client.get(
            BASE_URL,
            params=BOUNDING_BOX,
            headers=HEADERS,
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        states = data.get("states", [])
        logger.info(f"Fetched {len(states)} aircraft positions")
        return states

    except httpx.HTTPError as e:
        logger.error(f"HTTP error fetching positions: {e}")
        return []

async def run():
    logger.info(f"Starting OpenSky ingestion")
    fetched_at = datetime.now(timezone.utc).isoformat()

    async with httpx.AsyncClient() as client:
        states = await fetch_positions(client)
        enriched_records = [enrich_position(state, fetched_at) for state in states]
        write_parquet(enriched_records, "positions")
        logger.info(f"OpenSky ingestion finished, {len(enriched_records)} records written.")        

if __name__ == "__main__":
    asyncio.run(run())