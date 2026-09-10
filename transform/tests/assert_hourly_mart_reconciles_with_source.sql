-- Every observation that reached the intermediate model must be counted once,
-- and only once, in the hourly mart.
--
-- This is the test that catches the late-arrival failure mode: if data lands
-- for an hour older than late_arrival_lookback_hours, that hour is never
-- rebuilt and its counts go stale. Rather than let the mart quietly disagree
-- with its source, this fails and tells you to --full-refresh.
--
-- The current hour is excluded. Ingestion writes continuously, so an hour that
-- is still filling can legitimately differ between the moment the mart was
-- built and the moment this test runs. Comparing only closed hours makes the
-- test meaningful instead of flaky.

with mart as (

    select
        activity_hour,
        sum(observation_count) as mart_observations
    from {{ ref('fct_airspace_activity_hourly') }}
    where activity_hour < date_trunc('hour', current_timestamp)
    group by 1

),

expected as (

    select
        observed_hour as activity_hour,
        count(*)      as source_observations
    from {{ ref('int_state_vectors__gridded') }}
    where observed_hour < date_trunc('hour', current_timestamp)
    group by 1

)

select
    coalesce(mart.activity_hour, expected.activity_hour) as activity_hour,
    mart.mart_observations,
    expected.source_observations

from mart
full outer join expected
    on mart.activity_hour = expected.activity_hour

where coalesce(mart.mart_observations, -1)
      <> coalesce(expected.source_observations, -1)
