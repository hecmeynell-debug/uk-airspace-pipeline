-- Ingestion runs, cleaned. Provides the referential anchor for provenance
-- tests and the raw material for the pipeline-health metrics Phase 3 adds.
--
-- Note this describes the *pipeline*, never the airspace. Any alerting built
-- on top of it is about freshness, row counts and error rates - not content.

with source as (

    select * from {{ source('raw', 'ingestion_runs') }}

),

cleaned as (

    select
        run_id,
        source                                          as source_system,
        source_endpoint,
        request_bbox,
        auth_mode,
        credits_estimated,

        started_at,
        finished_at,
        date_trunc('hour', started_at)                  as started_hour,
        extract(epoch from (finished_at - started_at))  as duration_seconds,

        status,
        status = 'succeeded'                            as is_successful,
        http_status,

        api_snapshot_time,
        -- Lag between the snapshot the API served and when we finished storing
        -- it: the pipeline's end-to-end latency for that run.
        extract(epoch from (finished_at - api_snapshot_time))
                                                        as end_to_end_lag_seconds,

        coalesce(rows_received, 0)                      as rows_received,
        coalesce(rows_rejected, 0)                      as rows_rejected,
        coalesce(rows_inserted, 0)                      as rows_inserted,
        coalesce(rows_duplicate, 0)                     as rows_duplicate,
        error_message

    from source

)

select * from cleaned
