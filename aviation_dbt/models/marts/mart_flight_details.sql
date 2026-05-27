{{ config(
    materialized='external',
    location='s3://aviation-lakehouse/marts/flight_details',
    format='parquet'
) }}

select
    flight_date,
    flight_status,
    flight_iata,
    flight_number,
    dep_iata,
    dep_scheduled,
    dep_actual,
    dep_delay_minutes,
    arr_iata,
    arr_scheduled,
    arr_actual,
    arr_delay_minutes,
    total_delay_minutes,
    duration_minutes,
    airline_iata,
    airline_icao,
    aircraft_icao,
    arr_flight_category                     as weather_category,
    arr_temp_c,
    arr_wind_speed_knots,
    arr_wind_gust_knots,
    arr_visibility,
    arr_weather_phenomena,
    arr_sky_cover,
    fetched_at
from {{ ref('int_flights_with_weather') }}
order by flight_date desc, arr_scheduled