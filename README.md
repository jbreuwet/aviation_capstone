# Flight Delay & Cancellation Pipeline

A real-time data engineering pipeline that tracks flight delays and cancellations across 7 major US airports, correlates them with live weather conditions, and surfaces the results through a dashboard.

---

## What it does

Every hour, the pipeline pulls flight schedule data from the AirLabs API for arrivals and departures at EWR, ORD, SFO, JFK, LAX, ATL, and DFW. At the same time, NOAA weather observations are pulled every 30 minutes, and OpenSky Network position data is collected every minute. All of it lands in Cloudflare R2 as Parquet files.

From there, dbt transforms the raw data through a staging and intermediate layer before writing clean mart tables back to R2. Those mart tables get loaded into a local DuckLake catalog, which Evidence reads to power the dashboards.

The whole thing is orchestrated by Prefect running in Docker.

---

## Architecture

```
AirLabs API  ──────────────────────────────────────────┐
OpenSky Network  ────────────── Prefect ───────────────► Cloudflare R2 (raw/)
NOAA Aviation Weather  ────────────────────────────────┘
                                                              │
                                                           dbt run
                                                              │
                                                            Prefect
                                                              |
                                                              ▼
                                                    Cloudflare R2 (marts/)
                                                              │
                                                     ducklake_export.py
                                                              |
                                                            Prefect
                                                              │
                                                              ▼
                                              DuckLake (lakehouse/ducklake/)
                                               Postgres catalog  +  Parquet files
                                                              │
                                                              ▼
                                                    Evidence Dashboards
```

---

## Tech stack

| Layer | Tool |
|---|---|
| Ingestion | Python + httpx |
| Storage | Cloudflare R2 (S3-compatible) |
| Orchestration | Prefect (self-hosted, Docker) |
| Transformation | dbt + dbt-duckdb |
| Query engine | DuckDB |
| Lakehouse catalog | DuckLake (Postgres-backed) |
| Dashboards | Evidence |
| Infrastructure | Docker + Postgres |
| Package management | uv |

---

## Data sources

**AirLabs** (`/api/v9/schedules`) — flight schedules with departure and arrival times, delay minutes, gate info, and flight status. Polled hourly for arrivals and departures at each of the 7 monitored airports. Requires a paid API key ($49/month for 25k queries).

**OpenSky Network** — live aircraft position snapshots over the continental US bounding box. Collected every minute. Used to detect holding patterns and measure airport congestion. Free with registration.

**NOAA Aviation Weather** — METAR observations for the ICAO station at each monitored airport. Includes flight category (VFR/MVFR/IFR/LIFR), visibility, wind speed, temperature, and raw observation string. Free, no key required.

---

## dbt models

```
models/
├── staging/
│   ├── stg_flights.sql        # AirLabs schedules, deduplicated by flight + departure time
│   ├── stg_weather.sql        # NOAA METARs, type-cast and renamed
│   └── stg_positions.sql      # OpenSky state vectors, epoch timestamps converted
├── intermediate/
│   └── int_flights_with_weather.sql   # Flights left-joined to weather on arrival airport
└── marts/
    ├── mart_airport_delays.sql        # Aggregated delay + cancellation metrics by airport/date/weather
    ├── mart_flight_details.sql        # Row-level fact table with weather context
    ├── mart_weather_impact.sql        # Weather severity ranking for correlation analysis
    └── mart_airport_congestion.sql    # Aircraft count + holding pattern detection from positions
```

Staging and intermediate models materialize as views in DuckDB. Mart models write Parquet files directly to Cloudflare R2 via dbt-duckdb's external materialization.

---

## Prefect flows

Three deployments run on independent schedules:

- **airlabs-deployment** — every hour. Runs AirLabs ingestion → dbt run → dbt test → DuckLake export.
- **noaa-deployment** — every 30 minutes. Runs NOAA ingestion only.
- **opensky-deployment** — every minute. Runs OpenSky ingestion only.

The Prefect server runs in Docker and the deployments are served from the host using `uv run python src/flows/pipeline_flow.py`. The Prefect UI is available at `http://localhost:4200`.

---

## DuckLake

DuckLake sits between the R2 mart files and the Evidence dashboards. It uses Postgres as a metadata catalog — tracking which Parquet files belong to which tables, their schemas, and a transaction log for consistency. DuckDB then reads those files when queries come in.

This solves two problems that came up during development: concurrent access (multiple processes reading the same files without file lock conflicts) and schema consistency (enforcing that all files in a table conform to the same schema even as the pipeline evolves).

The `ducklake_export.py` script runs after each dbt run, reads the mart files from R2, and loads them into the local DuckLake catalog. Evidence connects to DuckLake through a junction symlink at `evidence/sources/aviation/`.

---

## Project structure

```
aviation_capstone/
├── src/
│   ├── ingestion/
│   │   ├── airlabs_ingestion.py
│   │   ├── opensky_ingestion.py
│   │   └── noaa_ingestion.py
│   ├── flows/
│   │   └── pipeline_flow.py
│   └── utils/
│       ├── parquet_writer.py
│       ├── ducklake_export.py
│       └── logger_config.py
├── aviation_dbt/
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
│   └── profiles.yml
├── evidence/
│   ├── pages/
│   │   ├── index.md           # Airport delay rankings
│   │   ├── flights.md         # Flight details + airline breakdown
│   │   ├── weather.md         # Weather impact analysis
│   │   └── congestion.md      # Aircraft congestion + holding patterns
│   └── sources/
│       └── aviation/
├── lakehouse/
│   ├── raw/                   # Local fallback (primary storage is R2)
│   └── ducklake/              # DuckLake Parquet files + Postgres catalog
├── docker-compose.yml
├── Dockerfile.prefect
└── pyproject.toml
```

---

## Setup

### Prerequisites

- Docker + Docker Compose
- Python 3.12+
- uv (`pip install uv`)
- Node.js 20+ (for Evidence)
- Cloudflare R2 bucket + API token
- AirLabs API key
- OpenSky Network account (free)

### Environment variables

Copy `.env.example` to `.env` and fill in your values:

```bash
POSTGRES_USER=admin
POSTGRES_PASSWORD=your_password
POSTGRES_DB=prefect
POSTGRES_HOST=localhost
POSTGRES_PORT=5432

AIRLABS_API_KEY=your_key
OPENSKY_USERNAME=your_username
OPENSKY_PASSWORD=your_password

R2_ACCOUNT_ID=your_account_id
R2_ACCESS_KEY_ID=your_access_key
R2_SECRET_ACCESS_KEY=your_secret_key
R2_BUCKET_NAME=your_bucket_name
R2_ENDPOINT_URL=your_account_id.r2.cloudflarestorage.com

DUCKLAKE_DATA_PATH=C:/path/to/lakehouse/ducklake
PREFECT_API_URL=http://localhost:4200/api
```

### Start infrastructure

```bash
docker compose up -d
```

### Install Python dependencies

```bash
uv sync
```

### Load environment variables (PowerShell)

```powershell
Get-Content .env | ForEach-Object {
    if ($_ -match '^\s*([^#][^=]+)=(.*)$') {
        $key = $matches[1].Trim()
        $value = $matches[2].Trim().Trim("'").Trim('"')
        [System.Environment]::SetEnvironmentVariable($key, $value, 'Process')
    }
}
```

### Run dbt

```bash
cd aviation_dbt
dbt run --profiles-dir .
dbt test --profiles-dir .
```

### Start Prefect deployments

```powershell
$env:PREFECT_API_URL="http://localhost:4200/api"
$env:PYTHONPATH="src"
uv run python src/flows/pipeline_flow.py
```

### Set up Evidence

```bash
cd evidence
npm install
npm run sources
npm run dev
```

Evidence dashboard available at `http://localhost:3000`.

---

## Monitored airports

| IATA | Airport | Known for |
|---|---|---|
| EWR | Newark Liberty International | Chronic delays, congested airspace |
| ORD | Chicago O'Hare International | Weather + volume pressure |
| SFO | San Francisco International | Fog-driven LIFR conditions |
| JFK | John F. Kennedy International | High volume, complex routes |
| LAX | Los Angeles International | West coast hub |
| ATL | Hartsfield-Jackson Atlanta | World's busiest airport |
| DFW | Dallas Fort Worth International | Major hub, storm exposure |

---

## Notes

- AirLabs returns a maximum of 100 flights per endpoint call. With 7 airports × 2 directions = 14 calls per hourly run, the $49/month Developer plan (25k queries) provides ample headroom.
- OpenSky rate limits anonymous requests aggressively. A registered account is required for reliable per-minute polling.
- The `normalize_mixed_columns()` function in `parquet_writer.py` handles AirLabs fields that return mixed types across records (null vs string vs integer) by coercing them to string at write time, preventing schema conflicts in downstream Parquet reads.
- `union_by_name=true` in the dbt staging models handles schema drift between Parquet files written at different points in the pipeline's development.