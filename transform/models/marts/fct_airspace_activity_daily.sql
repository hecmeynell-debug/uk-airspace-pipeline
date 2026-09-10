-- Daily traffic profile by altitude band.
--
-- Built from the intermediate model rather than rolled up from the hourly
-- mart, because distinct_aircraft cannot be summed: an airframe seen in three
-- hours is one airframe that day, not three. Summing the hourly counts would
-- silently inflate the figure. Recomputing costs another scan, and at this
-- volume correctness is worth more than the scan.
--
-- Materialised as a full table rather than incrementally. Daily grain is small
-- - a handful of rows per day - so a complete rebuild is both cheaper to
-- reason about and immune to the late-arrival problem the hourly mart has to
-- handle explicitly.

with observations as (

    select * from {{ ref('int_state_vectors__gridded') }}

),

aggregated as (

    select
        observed_date                            as activity_date,
        altitude_band,

        count(*)                                 as observation_count,
        count(distinct icao24)                   as distinct_aircraft,

        -- How much of the region this band was observed across. A coverage
        -- measure, not a location: it says how widely spread the traffic was.
        count(distinct (grid_lat, grid_lon))     as distinct_grid_cells,

        round(avg(baro_altitude_ft)::numeric, 0) as avg_baro_altitude_ft,
        round(avg(velocity_kts)::numeric, 1)     as avg_velocity_kts,

        min(observed_at)                         as first_observed_at,
        max(observed_at)                         as last_observed_at,
        count(distinct ingestion_run_id)         as contributing_run_count,
        max(ingested_at)                         as last_ingested_at

    from observations
    group by 1, 2

)

select * from aggregated
