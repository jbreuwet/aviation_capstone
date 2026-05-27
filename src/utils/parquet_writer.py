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

R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME")
R2_ENDPOINT_URL = os.getenv("R2_ENDPOINT_URL")
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")

USE_R2 = all([R2_BUCKET_NAME, R2_ENDPOINT_URL, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY])

def get_r2_client():
    endpoint = R2_ENDPOINT_URL
    if not endpoint.startswith("https://"):
        endpoint = f"https://{endpoint}"
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name="auto",
    )

# Coercing mixed datatype columns to string
def normalize_mixed_columns(df):
    for col in df.select_dtypes(include="object").columns:
        if df[col].apply(type).nunique() > 1:
            logger.warning(f"Mixed types detected in column '{col}' — coercing to string")
            df[col] = df[col].astype(str)
    # Also cast known volatile fields to string
    for col in ["live", "aircraft"]:
        if col in df.columns:
            df[col] = df[col].astype(str)
    return df


def write_parquet(records, source):
    if not records:
        logger.warning(f"No records to write for source '{source}' — skipping")
        return None

    records_df = pd.DataFrame(records)
    normalized_records = normalize_mixed_columns(records_df)

    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    timestamp_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    file_name = f"{source}_{timestamp_str}.parquet"
    object_key = f"raw/{source}/date={date_str}/{file_name}"

    # Write to Cloudflare R2 s3 Bucket
    if USE_R2:
        buffer = io.BytesIO()
        normalized_records.to_parquet(buffer, index=False, engine="pyarrow")
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

    # Fall back to local lakehouse
    else:
        output_dir = LAKEHOUSE_PATH / "raw" / source / f"date={date_str}"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / file_name
        normalized_records.to_parquet(output_path, index=False, engine="pyarrow")
        logger.info(f"Wrote {len(records)} records to {output_path}")
        return str(output_path)