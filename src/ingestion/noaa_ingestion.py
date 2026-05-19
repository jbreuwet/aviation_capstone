import asyncio
import httpx
from datetime import datetime, timezone
from logger_config import setup_logger
from dotenv import load_dotenv
from utils.parquet_writer import write_parquet

load_dotenv()
logger = setup_logger()

BASE_URL = "https://aviationweather.gov/api/data/metar"
HEADERS = {"User-Agent": "aviation-capstone-project/1.0"}

AIRPORTS = [
    "KEWR",  # Newark
    "KORD",  # Chicago O'Hare
    "KSFO",  # San Francisco
    "KJFK",  # New York JFK
    "KLAX",  # Los Angeles
    "KATL",  # Atlanta
    "KDFW",  # Dallas Fort Worth
]

def enrich_metar(record: dict, fetched_at: str):
    return {
        **record,
        "_fetched_at": fetched_at,
        "_source": "noaa_metar",
    }


async def fetch_metars(client: httpx.AsyncClient):
    ids = ",".join(AIRPORTS)
    params = {
        "ids": ids,
        "format": "json",
    }

    try:
        response = await client.get(
            BASE_URL,
            params=params,
            headers=HEADERS,
            timeout=30,
        )
        response.raise_for_status()
        records = response.json()
        logger.info(f"Fetched {len(records)} METAR records")
        return records

    except httpx.HTTPError as e:
        logger.error(f"HTTP error fetching METARs: {e}")
        return []


async def run():
    logger.info("Starting NOAA METAR ingestion")
    fetched_at = datetime.now(timezone.utc).isoformat()


    async with httpx.AsyncClient() as client:
        records = await fetch_metars(client)

    enriched_records = [enrich_metar(record, fetched_at) for record in records]
    write_parquet(enriched_records, "weather")
    logger.info(f"NOAA ingestion finished — published {len(enriched_records)} records")

if __name__ == "__main__":
    asyncio.run(run())