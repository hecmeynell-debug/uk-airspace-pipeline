-- Assigns each positioned observation to a coarse spatial cell and an altitude
-- band, so the marts can aggregate over stable buckets.
--
-- This is the last model in which per-airframe identity exists. It survives
-- here only so that the marts can compute count(distinct icao24); no column
-- carrying identity crosses into a mart, and a test enforces that.
--
-- Grid resolution is a var (default 1 degree). Coarse is the point: the output
-- describes where traffic is dense, not where any aircraft is.

{% set grid = var('grid_degrees') %}

with positioned as (

    select *
    from {{ ref('stg_opensky__state_vectors') }}
    where has_position

),

gridded as (

    select
        icao24,
        observed_at,
        observed_hour,
        observed_date,

        floor(latitude  / {{ grid }}) * {{ grid }} as grid_lat,
        floor(longitude / {{ grid }}) * {{ grid }} as grid_lon,

        case
            when on_ground                      then 'ground'
            when baro_altitude_ft is null       then 'unknown'
            when baro_altitude_ft <  10000      then 'below_10k'
            when baro_altitude_ft <  24000      then '10k_to_24k'
            when baro_altitude_ft <  35000      then '24k_to_35k'
            else                                     'above_35k'
        end as altitude_band,

        baro_altitude_ft,
        velocity_kts,
        on_ground,

        ingestion_run_id,
        ingested_at

    from positioned

)

select * from gridded
