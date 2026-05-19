# src/utils/parquet_writer.py
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
from logger_config import setup_logger

logger = setup_logger()

LAKEHOUSE_PATH = Path("lakehouse")


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

    output_dir = LAKEHOUSE_PATH / "raw" / source / f"date={date_str}"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"{source}_{timestamp_str}.parquet"
    df.to_parquet(output_path, index=False, engine="pyarrow")

    logger.info(f"Wrote {len(df)} records to {output_path}")
    return output_path