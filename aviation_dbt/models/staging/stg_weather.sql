with source as (
    select
        icaoId,
        obsTime,
        reportTime,
        temp,
        dewp,
        wdir,
        wspd,
        visib,
        wxString,
        cover,
        clouds,
        fltCat,
        altim,
        slp,
        lat,
        lon,
        name,
        rawOb,
        _fetched_at,
        _source
    from read_parquet(
        's3://{{ env_var("R2_BUCKET_NAME") }}/raw/weather/**/*.parquet',
        union_by_name=true
    )
    where icaoId is not null
)

select
    icaoId                                              as icao_id,
    epoch_ms(obsTime * 1000)::timestamp                 as obs_timestamp,
    reportTime::timestamp                               as report_time,
    temp                                                as temp_c,
    dewp                                                as dewpoint_c,
    wdir                                                as wind_dir,
    wspd::integer                                       as wind_speed_knots,
    null::integer                                       as wind_gust_knots,
    visib                                               as visibility,
    wxString                                            as weather_phenomena,
    cover                                               as sky_cover,
    clouds                                              as cloud_layers,
    fltCat                                              as flight_category,
    altim                                               as altimeter_hpa,
    slp                                                 as sea_level_pressure_hpa,
    lat                                                 as latitude,
    lon                                                 as longitude,
    name                                                as station_name,
    rawOb                                               as raw_observation,
    _fetched_at::timestamp                              as fetched_at,
    _source                                             as source

from source