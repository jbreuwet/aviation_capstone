-- models/marts/mart_airport_delays.sql
{{ config(
    materialized='external',
    location='s3://aviation-lakehouse/marts/airport_delays',
    format='parquet'
) }}

with flights as (
    select
        flight_date,
        flight_status,
        flight_iata,
        arr_iata                            as airport_iata,
        arr_delay_minutes,
        dep_iata                            as origin_iata,
        airline_iata,
        arr_flight_category                 as weather_category,
        arr_temp_c,
        arr_wind_speed_knots,
        arr_visibility,
        arr_sky_cover,
        fetched_at
    from {{ ref('int_flights_with_weather') }}
),

aggregated as (
    select
        airport_iata,
        flight_date,
        weather_category,
        count(*)                            as total_flights,
        count(case when flight_status = 'cancelled' then 1 end)
                                            as cancelled_flights,
        count(case when arr_delay_minutes > 0 then 1 end)
                                            as delayed_flights,
        avg(arr_delay_minutes)              as avg_delay_minutes,
        max(arr_delay_minutes)              as max_delay_minutes,
        avg(arr_temp_c)                     as avg_temp_c,
        avg(arr_wind_speed_knots)           as avg_wind_speed_knots
    from flights
    group by
        airport_iata,
        flight_date,
        weather_category
)

select
    airport_iata,
    flight_date,
    weather_category,
    total_flights,
    cancelled_flights,
    delayed_flights,
    round(avg_delay_minutes, 2)             as avg_delay_minutes,
    max_delay_minutes,
    round(cancelled_flights * 100.0 / nullif(total_flights, 0), 2)
                                            as cancellation_rate_pct,
    round(delayed_flights * 100.0 / nullif(total_flights, 0), 2)
                                            as delay_rate_pct,
    round(avg_temp_c, 1)                    as avg_temp_c,
    round(avg_wind_speed_knots, 1)          as avg_wind_speed_knots
from aggregated
order by flight_date desc, total_flights desc