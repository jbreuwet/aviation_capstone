-- models/staging/stg_flights.sql
with source as (
    select
        airline_iata,
        airline_icao,
        flight_iata,
        flight_icao,
        flight_number,
        dep_iata,
        dep_icao,
        dep_terminal,
        dep_gate,
        dep_time,
        dep_time_utc,
        dep_estimated,
        dep_actual,
        dep_actual_utc,
        arr_iata,
        arr_icao,
        arr_terminal,
        arr_gate,
        arr_baggage,
        arr_time,
        arr_time_utc,
        arr_estimated,
        arr_actual,
        arr_actual_utc,
        cs_airline_iata,
        cs_flight_iata,
        status,
        duration,
        delayed,
        dep_delayed,
        arr_delayed,
        aircraft_icao,
        arr_time_ts,
        dep_time_ts,
        _fetched_at,
        _source
    from read_parquet(
        's3://{{ env_var("R2_BUCKET_NAME") }}/raw/flights/**/*.parquet',
        union_by_name=true
    )
    where flight_iata is not null
),

deduplicated as (
    select *,
        row_number() over (
            partition by flight_iata, dep_time_utc
            order by _fetched_at desc
        ) as row_num
    from source
)

select
    -- flight identity
    flight_iata,
    flight_icao,
    flight_number,

    -- airline
    airline_iata,
    airline_icao,

    -- departure
    dep_iata,
    dep_icao,
    dep_terminal,
    dep_gate,
    dep_time::timestamp                     as dep_scheduled,
    dep_time_utc::timestamp                 as dep_scheduled_utc,
    dep_estimated::timestamp                as dep_estimated,
    dep_actual::timestamp                   as dep_actual,
    dep_delayed::integer                    as dep_delay_minutes,

    -- arrival
    arr_iata,
    arr_icao,
    arr_terminal,
    arr_gate,
    arr_baggage,
    arr_time::timestamp                     as arr_scheduled,
    arr_time_utc::timestamp                 as arr_scheduled_utc,
    arr_estimated::timestamp                as arr_estimated,
    arr_actual::timestamp                   as arr_actual,
    arr_delayed::integer                    as arr_delay_minutes,

    -- flight status
    status                                  as flight_status,
    duration::integer                       as duration_minutes,
    delayed::integer                        as total_delay_minutes,

    -- codeshare
    cs_airline_iata,
    cs_flight_iata,

    -- aircraft
    aircraft_icao,

    -- timestamps
    epoch_ms(dep_time_ts * 1000)::date      as flight_date,
    _fetched_at::timestamp                  as fetched_at,
    _source                                 as source

from deduplicated
where row_num = 1