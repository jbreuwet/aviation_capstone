# src/flows/pipeline_flow.py
import os
import subprocess
import sys
from pathlib import Path
from prefect import flow, task, get_run_logger, serve


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_PATH = PROJECT_ROOT / "src"


@task(name="aviationstack-ingestion", retries=2, retry_delay_seconds=30)
def run_aviationstack():
    logger = get_run_logger()
    logger.info("Running AviationStack ingestion")
    result = subprocess.run(
        [sys.executable, str(SRC_PATH / "ingestion" / "aviationstack_ingestion.py")],
        capture_output=True, text=True,
        env={**os.environ, "PYTHONPATH": str(SRC_PATH)},
    )
    if result.returncode != 0:
        raise RuntimeError(f"AviationStack ingestion failed:\n{result.stderr}")
    logger.info(result.stdout)


@task(name="opensky-ingestion", retries=2, retry_delay_seconds=30)
def run_opensky():
    logger = get_run_logger()
    logger.info("Running OpenSky ingestion")
    result = subprocess.run(
        [sys.executable, str(SRC_PATH / "ingestion" / "opensky_ingestion.py")],
        capture_output=True, text=True,
        env={**os.environ, "PYTHONPATH": str(SRC_PATH)},
    )
    if result.returncode != 0:
        raise RuntimeError(f"OpenSky ingestion failed:\n{result.stderr}")
    logger.info(result.stdout)


@task(name="noaa-ingestion", retries=2, retry_delay_seconds=30)
def run_noaa():
    logger = get_run_logger()
    logger.info("Running NOAA ingestion")
    result = subprocess.run(
        [sys.executable, str(SRC_PATH / "ingestion" / "noaa_ingestion.py")],
        capture_output=True, text=True,
        env={**os.environ, "PYTHONPATH": str(SRC_PATH)},
    )
    if result.returncode != 0:
        raise RuntimeError(f"NOAA ingestion failed:\n{result.stderr}")
    logger.info(result.stdout)


@task(name="dbt-run", retries=1, retry_delay_seconds=60)
def run_dbt():
    logger = get_run_logger()
    logger.info("Running dbt")
    result = subprocess.run(
        ["dbt", "run", "--profiles-dir", "."],
        capture_output=True, text=True,
        cwd=str(PROJECT_ROOT / "dbt"),
        env={**os.environ},
    )
    if result.returncode != 0:
        raise RuntimeError(f"dbt run failed:\n{result.stderr}")
    logger.info(result.stdout)


@task(name="dbt-test", retries=1, retry_delay_seconds=60)
def run_dbt_test():
    logger = get_run_logger()
    logger.info("Running dbt test")
    result = subprocess.run(
        ["dbt", "test", "--profiles-dir", "."],
        capture_output=True, text=True,
        cwd=str(PROJECT_ROOT / "dbt"),
        env={**os.environ},
    )
    if result.returncode != 0:
        raise RuntimeError(f"dbt test failed:\n{result.stderr}")
    logger.info(result.stdout)

@task(name="ducklake-export", retries=1, retry_delay_seconds=30)
def run_ducklake_export():
    logger = get_run_logger()
    logger.info("Running DuckLake export")
    result = subprocess.run(
        [sys.executable, str(SRC_PATH / "utils" / "ducklake_export.py")],
        capture_output=True, text=True,
        env={**os.environ, "PYTHONPATH": str(SRC_PATH)},
    )
    if result.returncode != 0:
        raise RuntimeError(f"DuckLake export failed:\n{result.stderr}")
    logger.info(result.stdout)


@flow(name="ingest-aviationstack", log_prints=True)
def aviationstack_flow():
    run_aviationstack()
    run_dbt()
    run_dbt_test()
    run_ducklake_export()


@flow(name="ingest-opensky", log_prints=True)
def opensky_flow():
    run_opensky()


@flow(name="ingest-noaa", log_prints=True)
def noaa_flow():
    run_noaa()


if __name__ == "__main__":
    aviationstack_deployment = aviationstack_flow.to_deployment(
        name="aviationstack-deployment",
        cron="0 * * * *",   # every 1 hours
    )
    opensky_deployment = opensky_flow.to_deployment(
        name="opensky-deployment",
        cron="* * * * *",   # every minute
    )
    noaa_deployment = noaa_flow.to_deployment(
        name="noaa-deployment",
        cron="*/30 * * * *",  # every 30 minutes
    )

    serve(aviationstack_deployment, opensky_deployment, noaa_deployment)