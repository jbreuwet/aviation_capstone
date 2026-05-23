-- models/marts/mart_airport_congestion.sql
{{ config(
    materialized='external',
    location='s3://aviation-lakehouse/marts/airport_congestion',
    format='parquet'
) }}

with airport_boxes as (
    select * from (values
        ('EWR', 'Newark Liberty International',      40.3, 41.2, -74.6, -73.7),
        ('ORD', 'Chicago O''Hare International',     41.6, 42.3, -88.2, -87.5),
        ('SFO', 'San Francisco International',       37.3, 37.9, -122.6, -122.0),
        ('JFK', 'John F Kennedy International',      40.4, 41.0, -74.1, -73.6),
        ('LAX', 'Los Angeles International',         33.6, 34.3, -118.7, -118.1),
        ('ATL', 'Hartsfield-Jackson Atlanta',        33.4, 34.1, -84.8, -84.2),
        ('DFW', 'Dallas Fort Worth International',   32.6, 33.3, -97.4, -96.8)
    ) t(airport_iata, airport_name, lat_min, lat_max, lon_min, lon_max)
),

positions as (
    select
        icao24,
        callsign,
        position_timestamp,
        latitude,
        longitude,
        baro_altitude,
        on_ground,
        velocity,
        vertical_rate,
        ingestion_date,
        fetched_at
    from {{ ref('stg_positions') }}
    where latitude is not null
      and longitude is not null
),

positions_with_airport as (
    select
        p.*,
        a.airport_iata,
        a.airport_name
    from positions p
    inner join airport_boxes a
        on p.latitude  between a.lat_min and a.lat_max
        and p.longitude between a.lon_min and a.lon_max
),

with_flags as (
    select
        *,
        case
            when on_ground = false
             and baro_altitude < 3000
             and abs(vertical_rate) < 2.0
            then true
            else false
        end                                 as possible_holding_pattern
    from positions_with_airport
),

aggregated as (
    select
        airport_iata,
        airport_name,
        ingestion_date,
        fetched_at::date                    as snapshot_date,
        date_trunc('hour', position_timestamp) as hour_window,
        count(distinct icao24)              as aircraft_count,
        count(distinct case when on_ground = true then icao24 end)
                                            as aircraft_on_ground,
        count(distinct case when on_ground = false then icao24 end)
                                            as aircraft_airborne,
        count(distinct case when possible_holding_pattern = true then icao24 end)
                                            as possible_holding,
        avg(baro_altitude) filter (where on_ground = false)
                                            as avg_airborne_altitude,
        avg(velocity) filter (where on_ground = false)
                                            as avg_airborne_velocity
    from with_flags
    group by
        airport_iata,
        airport_name,
        ingestion_date,
        snapshot_date,
        hour_window
)

select
    airport_iata,
    airport_name,
    hour_window,
    snapshot_date,
    aircraft_count,
    aircraft_on_ground,
    aircraft_airborne,
    possible_holding,
    round(avg_airborne_altitude, 0)         as avg_airborne_altitude_ft,
    round(avg_airborne_velocity, 1)         as avg_airborne_velocity_ms
from aggregated
order by hour_window desc, aircraft_count desc