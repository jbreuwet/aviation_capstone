# src/utils/parquet_writer.py
import io
import os
import boto3
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv
from logger_config import setup_logger

load_dotenv()
logger = setup_logger()

LAKEHOUSE_PATH = Path("lakehouse")

R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME", "").strip("'").strip('"')
R2_ENDPOINT_URL = os.getenv("R2_ENDPOINT_URL", "").strip("'").strip('"')
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID", "").strip("'").strip('"')
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY", "").strip("'").strip('"')

USE_R2 = all([R2_BUCKET_NAME, R2_ENDPOINT_URL, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY])

def get_r2_client():
    endpoint = R2_ENDPOINT_URL
    if not endpoint.startswith("https://"):
        endpoint = f"https://{endpoint}"
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=R2_ACCESS_KEY_ID.strip("'").strip('"'),
        aws_secret_access_key=R2_SECRET_ACCESS_KEY.strip("'").strip('"'),
        region_name="auto",
    )

def normalize_mixed_columns(df: pd.DataFrame):
    """
    Coerce any object columns with mixed types to string.
    This preserves raw data while making it Parquet-compatible.
    """
    for col in df.select_dtypes(include="object").columns:
        if df[col].apply(type).nunique() > 1:
            logger.warning(f"Mixed types detected in column '{col}' — coercing to string")
            df[col] = df[col].astype(str)
    return df


def write_parquet(records: list[dict], source: str) -> Path:
    """Write a list of records to a partitioned Parquet file in the lakehouse raw zone."""
    if not records:
        logger.warning(f"No records to write for source '{source}' — skipping")
        return None

    df = pd.DataFrame(records)
    df = normalize_mixed_columns(df)

    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    timestamp_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    file_name = f"{source}_{timestamp_str}.parquet"
    object_key = f"raw/{source}/date={date_str}/{file_name}"

    if USE_R2:
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False, engine="pyarrow")
        buffer.seek(0)
        
        r2_client = get_r2_client()
        r2_client.put_object(
            Bucket=R2_BUCKET_NAME,
            Key=object_key,
            Body=buffer.getvalue(),
        )
    
        destination = f"s3://{R2_BUCKET_NAME}/{object_key}"
        logger.info(f"Wrote {len(records)} records to {destination}")
        return destination

    else:
        # Fall back to local lakehouse
        output_dir = LAKEHOUSE_PATH / "raw" / source / f"date={date_str}"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / file_name
        df.to_parquet(output_path, index=False, engine="pyarrow")
        logger.info(f"Wrote {len(df)} records to {output_path}")
        return str(output_path)