with source as (
    select
        icao24,
        callsign,
        origin_country,
        time_position,
        last_contact,
        longitude,
        latitude,
        baro_altitude,
        on_ground,
        velocity,
        true_track,
        vertical_rate,
        geo_altitude,
        squawk,
        spi,
        position_source,
        _fetched_at,
        _source,
        date
    from read_parquet(
        's3://{{ env_var("R2_BUCKET_NAME") }}/raw/positions/**/*.parquet',
        union_by_name=true
    )
    where icao24 is not null
)

select
    icao24,
    trim(callsign)                              as callsign,
    origin_country,
    epoch_ms(time_position * 1000)::timestamp   as position_timestamp,
    epoch_ms(last_contact * 1000)::timestamp    as last_contact_timestamp,
    longitude,
    latitude,
    baro_altitude,
    on_ground,
    velocity,
    true_track,
    vertical_rate,
    geo_altitude,
    squawk,
    spi,
    position_source,
    _fetched_at::timestamp                      as fetched_at,
    _source                                     as source,
    date                                        as ingestion_date

from source