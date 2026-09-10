-- Light, lossless cleaning of the landing zone. One row in, one row out.
--
-- Staging deliberately does no filtering and no aggregation: it renames, casts
-- and derives, so that anything surprising downstream can be traced to exactly
-- one transformation. Rows without a position are kept and flagged rather than
-- dropped, because "how much of the feed lacks a position" is itself a data
-- quality measure worth being able to ask.

with source as (

    select * from {{ source('raw', 'state_vectors') }}

),

cleaned as (

    select
        icao24,
        observed_at,
        date_trunc('hour', observed_at)                as observed_hour,
        date_trunc('day', observed_at)::date           as observed_date,
        time_position,
        last_contact,

        latitude,
        longitude,

        -- Aviation works in feet; the API reports metres. Converting once here
        -- keeps the altitude banding downstream readable.
        baro_altitude_m,
        geo_altitude_m,
        baro_altitude_m * 3.280839895                  as baro_altitude_ft,
        geo_altitude_m  * 3.280839895                  as geo_altitude_ft,

        on_ground,
        velocity_ms,
        velocity_ms * 1.943844                         as velocity_kts,
        true_track_deg,
        vertical_rate_ms,
        position_source,
        category,

        (latitude is not null and longitude is not null) as has_position,

        -- provenance, carried through unchanged
        ingestion_run_id,
        api_snapshot_time,
        ingested_at,

        -- How stale the observation already was when the API served it. Useful
        -- for describing feed quality, not for anything about the aircraft.
        extract(epoch from (api_snapshot_time - observed_at))
            as observation_age_seconds

    from source

)

select * from cleaned
