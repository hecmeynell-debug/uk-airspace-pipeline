-- Hourly traffic density by grid cell and altitude band.
--
-- Aggregate and descriptive only. Every row answers "how much was observed
-- here, in this hour" - never which aircraft, never where one went next.
--
-- Late-arriving data
-- ------------------
-- Observations can land well after the hour they belong to: a poll at 12:01
-- returns positions timestamped 11:58, and a retry after an outage can land
-- much older ones. Filtering incrementally on ingested_at would pick those
-- rows up but leave the *hour* they belong to already built and now wrong.
--
-- So the incremental filter is on observed_at with a lookback window, and the
-- strategy is delete+insert keyed on activity_hour: every affected hour is
-- deleted and rebuilt wholesale rather than appended to. Rebuilding an hour is
-- idempotent, which matters more here than doing the least possible work.
--
-- Anything arriving later than the lookback window is not silently half-merged;
-- it needs a --full-refresh, and that is the honest failure mode.

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'activity_hour',
    on_schema_change = 'fail'
) }}

with observations as (

    select * from {{ ref('int_state_vectors__gridded') }}

    {% if is_incremental() %}
    where observed_at >= (
        select coalesce(
            max(activity_hour) - interval '{{ var("late_arrival_lookback_hours") }} hours',
            '-infinity'::timestamptz
        )
        from {{ this }}
    )
    {% endif %}

),

aggregated as (

    select
        observed_hour                          as activity_hour,
        grid_lat,
        grid_lon,
        altitude_band,

        count(*)                               as observation_count,
        -- An aggregate, not a list. This is the only thing identity is used
        -- for, and it does not survive the group by.
        count(distinct icao24)                 as distinct_aircraft,

        round(avg(baro_altitude_ft)::numeric, 0)  as avg_baro_altitude_ft,
        round(avg(velocity_kts)::numeric, 1)      as avg_velocity_kts,

        min(observed_at)                       as first_observed_at,
        max(observed_at)                        as last_observed_at,

        -- Provenance survives aggregation: how many ingestion runs contributed
        -- to this bucket, and when it was last touched.
        count(distinct ingestion_run_id)       as contributing_run_count,
        max(ingested_at)                       as last_ingested_at

    from observations
    group by 1, 2, 3, 4

)

select * from aggregated
