-- Raw landing zone for public ADS-B state vectors.
--
-- Two tables: one row per ingestion attempt (provenance and observability),
-- and one row per distinct observation. The landing zone is append-only -
-- airspace_ingest is granted INSERT on state_vectors and never UPDATE or
-- DELETE, so a load can add history but can never rewrite it.

-- ---------------------------------------------------------------------------
-- Ingestion runs: the provenance spine. Every landed row points at the run
-- that first observed it, and the run records exactly what was asked of the
-- API and what came back.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.ingestion_runs (
    run_id             uuid        PRIMARY KEY,
    source             text        NOT NULL DEFAULT 'opensky_network',
    source_endpoint    text        NOT NULL,
    request_bbox       jsonb       NOT NULL,
    auth_mode          text        NOT NULL,
    credits_estimated  integer     NOT NULL,
    started_at         timestamptz NOT NULL DEFAULT now(),
    finished_at        timestamptz,
    status             text        NOT NULL DEFAULT 'running',
    http_status        integer,
    api_snapshot_time  timestamptz,
    rows_received      integer,
    rows_rejected      integer,
    rows_inserted      integer,
    rows_duplicate     integer,
    error_message      text,

    CONSTRAINT ingestion_runs_status_chk
        CHECK (status IN ('running', 'succeeded', 'failed')),
    CONSTRAINT ingestion_runs_auth_mode_chk
        CHECK (auth_mode IN ('anonymous', 'oauth2_client_credentials')),
    -- A finished run must say when it finished; a running one must not.
    CONSTRAINT ingestion_runs_finished_chk
        CHECK ((status = 'running') = (finished_at IS NULL))
);

COMMENT ON TABLE raw.ingestion_runs IS
    'One row per ingestion attempt. Provenance and observability spine: every '
    'row in raw.state_vectors references the run that first observed it.';

CREATE INDEX IF NOT EXISTS ingestion_runs_started_at_idx
    ON raw.ingestion_runs (started_at DESC);
CREATE INDEX IF NOT EXISTS ingestion_runs_status_idx
    ON raw.ingestion_runs (status)
    WHERE status <> 'succeeded';

-- ---------------------------------------------------------------------------
-- State vectors.
--
-- The primary key (icao24, observed_at) is what makes re-ingestion of the same
-- window a no-op. observed_at is time_position where the API reported one, and
-- last_contact otherwise, so every row is addressable. Polling every 120s while
-- positions update every 5s means consecutive snapshots normally carry
-- different observed_at values; when an aircraft has not moved or reported,
-- the repeat collapses onto the existing row instead of duplicating it.
--
-- icao24 is stored because deduplication and provenance require a natural key.
-- It does not leave this schema: no mart exposes per-airframe identity.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.state_vectors (
    icao24             text             NOT NULL,
    observed_at        timestamptz      NOT NULL,
    time_position      timestamptz,
    last_contact       timestamptz      NOT NULL,
    callsign           text,
    origin_country     text,
    longitude          double precision,
    latitude           double precision,
    baro_altitude_m    double precision,
    geo_altitude_m     double precision,
    on_ground          boolean          NOT NULL,
    velocity_ms        double precision,
    true_track_deg     double precision,
    vertical_rate_ms   double precision,
    squawk             text,
    spi                boolean          NOT NULL DEFAULT false,
    position_source    smallint,
    category           smallint,

    -- provenance, on every single record
    ingestion_run_id   uuid             NOT NULL REFERENCES raw.ingestion_runs (run_id),
    api_snapshot_time  timestamptz      NOT NULL,
    ingested_at        timestamptz      NOT NULL DEFAULT now(),

    CONSTRAINT state_vectors_pkey PRIMARY KEY (icao24, observed_at),

    -- Defence in depth: the same bounds the Python parser enforces, restated
    -- where they cannot be bypassed by a future writer.
    CONSTRAINT state_vectors_latitude_chk
        CHECK (latitude IS NULL OR latitude BETWEEN -90 AND 90),
    CONSTRAINT state_vectors_longitude_chk
        CHECK (longitude IS NULL OR longitude BETWEEN -180 AND 180),
    CONSTRAINT state_vectors_true_track_chk
        CHECK (true_track_deg IS NULL OR (true_track_deg >= 0 AND true_track_deg < 360)),
    CONSTRAINT state_vectors_icao24_chk
        CHECK (icao24 = lower(icao24) AND length(icao24) BETWEEN 1 AND 12)
);

COMMENT ON TABLE raw.state_vectors IS
    'Append-only landing zone for public ADS-B state vectors. One row per '
    'distinct (icao24, observed_at). No transformation applied.';
COMMENT ON COLUMN raw.state_vectors.observed_at IS
    'time_position where reported, else last_contact. Deduplication key.';
COMMENT ON COLUMN raw.state_vectors.ingestion_run_id IS
    'The run that FIRST observed this row. Re-ingestion does not overwrite it.';

CREATE INDEX IF NOT EXISTS state_vectors_observed_at_idx
    ON raw.state_vectors (observed_at DESC);
CREATE INDEX IF NOT EXISTS state_vectors_run_idx
    ON raw.state_vectors (ingestion_run_id);

-- ---------------------------------------------------------------------------
-- Grants.
--
-- Default privileges (set at bootstrap) already give airspace_ingest
-- SELECT+INSERT and airspace_transform SELECT on tables created here. The one
-- addition is UPDATE on ingestion_runs, because a run must be finalised with
-- its outcome. Note what is NOT granted: airspace_ingest has no UPDATE or
-- DELETE on state_vectors, so landed observations are immutable to it.
-- ---------------------------------------------------------------------------
GRANT UPDATE ON raw.ingestion_runs TO airspace_ingest;
