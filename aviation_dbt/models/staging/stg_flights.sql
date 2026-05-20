with source as (
    select
        flight_date,
        flight_status,
        departure,
        arrival,
        airline,
        flight,
        _fetched_at,
        _source
    from read_parquet(
        's3://{{ env_var("R2_BUCKET_NAME") }}/raw/flights/**/*.parquet'
    )
)

select
    -- identifiers
    flight_date::date                       as flight_date,
    flight_status,
    flight.iata::varchar                    as flight_iata,
    flight.number::varchar                  as flight_number,

    -- departure
    departure.iata::varchar                 as dep_iata,
    departure.airport::varchar              as dep_airport,
    departure.scheduled::varchar            as dep_scheduled,
    departure.actual::varchar               as dep_actual,
    departure.delay::integer                as dep_delay_minutes,

    -- arrival
    arrival.iata::varchar                   as arr_iata,
    arrival.airport::varchar                as arr_airport,
    arrival.scheduled::varchar              as arr_scheduled,
    arrival.actual::varchar                 as arr_actual,
    arrival.delay::integer                  as arr_delay_minutes,

    -- airline
    airline.name::varchar                   as airline_name,
    airline.iata::varchar                   as airline_iata,

    -- pipeline metadata
    _fetched_at::timestamp                  as fetched_at,
    _source                                 as source

from source
where flight_date is not null