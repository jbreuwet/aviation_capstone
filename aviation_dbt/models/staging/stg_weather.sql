-- models/staging/stg_weather.sql
with source as (
    select
        icaoId,
        obsTime,
        reportTime,
        temp,
        dewp,
        wdir,
        wspd,
        wgst,
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
        's3://{{ env_var("R2_BUCKET_NAME") }}/raw/weather/**/*.parquet'
    )
)

select
    -- identifiers
    icaoId                                              as icao_id,
    epoch_ms(obsTime * 1000)::timestamp                 as obs_timestamp,
    reportTime::timestamp                               as report_time,

    -- temperature
    temp                                                as temp_c,
    dewp                                                as dewpoint_c,

    -- wind
    wdir                                                as wind_dir,
    wspd::integer                                       as wind_speed_knots,
    case
        when wgst is null or isnan(wgst) then null
        else wgst::integer
    end                                                 as wind_gust_knots,

    -- visibility and conditions
    visib                                               as visibility,
    wxString                                            as weather_phenomena,
    cover                                               as sky_cover,
    clouds                                              as cloud_layers,
    fltCat                                              as flight_category,

    -- pressure
    altim                                               as altimeter_hpa,
    slp                                                 as sea_level_pressure_hpa,

    -- location
    lat                                                 as latitude,
    lon                                                 as longitude,
    name                                                as station_name,

    -- raw
    rawOb                                               as raw_observation,

    -- pipeline metadata
    _fetched_at::timestamp                              as fetched_at,
    _source                                             as source

from source
where icaoId is not null