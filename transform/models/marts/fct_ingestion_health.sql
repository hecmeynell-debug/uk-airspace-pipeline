-- Hourly pipeline health.
--
-- This mart describes the *pipeline*, never the airspace. Row counts, error
-- rates, latency and credit spend - the things you would page someone about.
-- CONSTRAINTS.md permits alerting on exactly this and nothing else: there is
-- deliberately no measure here derived from what the aircraft were doing.
--
-- Kept alongside the activity marts so the read-only role can see it: whoever
-- consumes the aggregates should be able to tell whether they are complete.

with runs as (

    select * from {{ ref('stg_opensky__ingestion_runs') }}

),

hourly as (

    select
        started_hour                                            as health_hour,

        count(*)                                                as run_count,
        count(*) filter (where status = 'succeeded')            as succeeded_count,
        count(*) filter (where status = 'failed')               as failed_count,
        count(*) filter (where status = 'running')              as still_running_count,

        round(
            100.0 * count(*) filter (where status = 'succeeded')
                  / nullif(count(*), 0)
        , 1)                                                    as success_rate_pct,

        sum(rows_received)                                      as rows_received,
        sum(rows_inserted)                                      as rows_inserted,
        sum(rows_duplicate)                                     as rows_duplicate,
        sum(rows_rejected)                                      as rows_rejected,

        -- Rejected rows are a data-quality signal about the feed, not about any
        -- aircraft: a rising rate means OpenSky is serving us malformed records.
        round(
            100.0 * sum(rows_rejected) / nullif(sum(rows_received), 0)
        , 3)                                                    as reject_rate_pct,

        round(avg(duration_seconds)::numeric, 3)                as avg_duration_seconds,
        round(avg(end_to_end_lag_seconds)::numeric, 1)          as avg_lag_seconds,
        round(max(end_to_end_lag_seconds)::numeric, 1)          as max_lag_seconds,

        -- Credit spend against the documented daily allowance. The one number
        -- that decides whether the configured cadence is sustainable.
        sum(credits_estimated)                                  as credits_consumed,

        max(finished_at)                                        as last_finished_at

    from runs
    group by 1

)

select * from hourly
