{{ config(
    materialized='external',
    location='s3://aviation-lakehouse/marts/weather_impact',
    format='parquet'
) }}

with base as (
    select
        arr_iata                            as airport_iata,
        flight_date,
        arr_flight_category                 as flight_category,
        arr_visibility                      as visibility,
        arr_wind_speed_knots                as wind_speed_knots,
        arr_temp_c                          as temp_c,
        arr_sky_cover                       as sky_cover,
        arr_delay_minutes                   as delay_minutes,
        flight_status,
        count(*) over (
            partition by arr_iata, arr_flight_category
        )                                   as flights_in_category
    from {{ ref('int_flights_with_weather') }}
    where arr_flight_category is not null
)

select
    airport_iata,
    flight_date,
    flight_category,
    visibility,
    wind_speed_knots,
    temp_c,
    sky_cover,
    delay_minutes,
    flight_status,
    flights_in_category,
    case
        when flight_category = 'VFR'  then 1
        when flight_category = 'MVFR' then 2
        when flight_category = 'IFR'  then 3
        when flight_category = 'LIFR' then 4
        else null
    end                                     as weather_severity_rank
from base
order by flight_date desc, weather_severity_rank desc