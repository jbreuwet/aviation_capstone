-- models/intermediate/int_flights_with_weather.sql
with flights as (
    select
        flight_date,
        flight_status,
        flight_iata,
        flight_number,
        dep_iata,
        dep_airport,
        dep_scheduled,
        dep_actual,
        dep_delay_minutes,
        arr_iata,
        arr_airport,
        arr_scheduled,
        arr_actual,
        arr_delay_minutes,
        airline_name,
        airline_iata,
        fetched_at,
        source
    from {{ ref('stg_flights') }}
),

weather as (
    select
        icao_id,
        obs_timestamp,
        temp_c,
        dewpoint_c,
        wind_dir,
        wind_speed_knots,
        wind_gust_knots,
        visibility,
        weather_phenomena,
        sky_cover,
        flight_category,
        altimeter_hpa,
        sea_level_pressure_hpa,
        station_name
    from {{ ref('stg_weather') }}
),

-- Join weather to flights on arrival airport
-- NOAA uses ICAO codes (KEWR) while AviationStack uses IATA (EWR)
-- so we strip the leading K from the ICAO code to match
flights_with_weather as (
    select
        f.flight_date,
        f.flight_status,
        f.flight_iata,
        f.flight_number,
        f.dep_iata,
        f.dep_airport,
        f.dep_scheduled,
        f.dep_actual,
        f.dep_delay_minutes,
        f.arr_iata,
        f.arr_airport,
        f.arr_scheduled,
        f.arr_actual,
        f.arr_delay_minutes,
        f.airline_name,
        f.airline_iata,

        -- weather at arrival airport
        w.obs_timestamp                         as arr_weather_obs_time,
        w.temp_c                                as arr_temp_c,
        w.dewpoint_c                            as arr_dewpoint_c,
        w.wind_dir                              as arr_wind_dir,
        w.wind_speed_knots                      as arr_wind_speed_knots,
        w.wind_gust_knots                       as arr_wind_gust_knots,
        w.visibility                            as arr_visibility,
        w.weather_phenomena                     as arr_weather_phenomena,
        w.sky_cover                             as arr_sky_cover,
        w.flight_category                       as arr_flight_category,

        f.fetched_at,
        f.source

    from flights f
    left join weather w
        on f.arr_iata = substr(w.icao_id, 2)
)

select * from flights_with_weather