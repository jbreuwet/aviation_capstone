import os
import duckdb
from dotenv import load_dotenv
from logger_config import setup_logger

load_dotenv()
logger = setup_logger()

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", 5432))
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASS = os.getenv("POSTGRES_PASSWORD")
DUCKLAKE_DATA_PATH = os.getenv("DUCKLAKE_DATA_PATH", "/lakehouse/ducklake")

R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_ENDPOINT_URL = os.getenv("R2_ENDPOINT_URL")
R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME")

MARTS = [
    "airport_delays",
    "flight_details",
    "weather_impact",
    "airport_congestion",
]


def get_duckdb_connection():
    conn = duckdb.connect()

    # Install and load required extensions
    conn.execute("INSTALL ducklake;")
    conn.execute("LOAD ducklake;")
    conn.execute("INSTALL httpfs;")
    conn.execute("LOAD httpfs;")

    # Configure R2 access
    conn.execute(f"""
        CREATE SECRET r2_secret (
            TYPE S3,
            KEY_ID '{R2_ACCESS_KEY_ID}',
            SECRET '{R2_SECRET_ACCESS_KEY}',
            ENDPOINT '{R2_ENDPOINT_URL}',
            REGION 'auto',
            USE_SSL true,
            URL_STYLE 'path'
        )
    """)

    # Attach DuckLake using Postgres as catalog
    conn.execute(f"""
        ATTACH 'ducklake:dbname=ducklake user={POSTGRES_USER} password={POSTGRES_PASS} host={POSTGRES_HOST} port={POSTGRES_PORT}'
        AS lake (DATA_PATH '{DUCKLAKE_DATA_PATH}', DATA_INLINING_ROW_LIMIT 0)
    """)
    conn.execute("USE lake;")

    logger.info("DuckLake connection established")
    return conn


def load_mart(conn, mart_name):
    path = f"s3://{R2_BUCKET_NAME}/marts/{mart_name}"

    logger.info(f"Loading mart_{mart_name} from R2")

    df = conn.execute(f"""
        SELECT * FROM read_parquet('{path}')
    """).df()

    logger.info(f"Read {len(df)} rows from {path}")

    conn.execute(f"DROP TABLE IF EXISTS mart_{mart_name}")
    conn.execute(f"""
        CREATE TABLE mart_{mart_name} AS
        SELECT * FROM df
    """)

    row_count = conn.execute(f"SELECT COUNT(*) FROM mart_{mart_name}").fetchone()[0]
    logger.info(f"Loaded {row_count} rows into DuckLake mart_{mart_name}")


def run():
    logger.info("Starting DuckLake export")
    conn = get_duckdb_connection()

    try:
        for mart in MARTS:
            load_mart(conn, mart)
        logger.info("DuckLake export complete")
    except Exception as e:
        logger.error(f"DuckLake export failed: {e}")
        raise
    finally:
        conn.close()
        logger.info("DuckLake connection closed")


if __name__ == "__main__":
    run()