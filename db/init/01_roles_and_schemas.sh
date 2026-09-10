#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Bootstrap least-privilege roles and the raw landing schema.
#
# Executed by the postgres image's entrypoint exactly once, on first start
# with an empty data directory. Every statement is written to be idempotent
# anyway, so the script can safely be re-run by hand against a live database.
#
# Passwords are passed as psql variables and interpolated with :'name', which
# applies proper single-quote escaping - never string-concatenated into DDL.
# ---------------------------------------------------------------------------
set -euo pipefail

psql -v ON_ERROR_STOP=1 \
     --username "$POSTGRES_USER" \
     --dbname "$POSTGRES_DB" \
     -v owner_pw="$AIRSPACE_OWNER_PASSWORD" \
     -v ingest_pw="$AIRSPACE_INGEST_PASSWORD" \
     -v transform_pw="$AIRSPACE_TRANSFORM_PASSWORD" \
     -v reader_pw="$AIRSPACE_READER_PASSWORD" \
     -v db="$POSTGRES_DB" <<-'ESQL'

	-- ---------------------------------------------------------------------
	-- Roles. CREATE ROLE has no IF NOT EXISTS, so generate the statement
	-- conditionally and execute it with \gexec.
	-- ---------------------------------------------------------------------
	SELECT format('CREATE ROLE airspace_owner LOGIN PASSWORD %L', :'owner_pw')
	WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'airspace_owner')\gexec

	SELECT format('CREATE ROLE airspace_ingest LOGIN PASSWORD %L', :'ingest_pw')
	WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'airspace_ingest')\gexec

	SELECT format('CREATE ROLE airspace_transform LOGIN PASSWORD %L', :'transform_pw')
	WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'airspace_transform')\gexec

	SELECT format('CREATE ROLE airspace_reader LOGIN PASSWORD %L', :'reader_pw')
	WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'airspace_reader')\gexec

	-- ---------------------------------------------------------------------
	-- Deny-by-default. Strip the permissive defaults Postgres ships with.
	-- ---------------------------------------------------------------------
	REVOKE ALL ON SCHEMA public FROM PUBLIC;
	SELECT format('REVOKE ALL ON DATABASE %I FROM PUBLIC', :'db')\gexec
	SELECT format('GRANT CONNECT ON DATABASE %I TO airspace_owner, airspace_ingest, airspace_transform, airspace_reader', :'db')\gexec

	-- ---------------------------------------------------------------------
	-- Landing zone. Owned by airspace_owner; DDL is applied as the owner in
	-- Phase 1 migrations, never by the ingest service account.
	-- ---------------------------------------------------------------------
	CREATE SCHEMA IF NOT EXISTS raw AUTHORIZATION airspace_owner;
	COMMENT ON SCHEMA raw IS
	  'Landing zone for public ADS-B state vectors. Append-only, with ingestion '
	  'timestamp and source provenance on every row. No transformation applied.';

	-- ingest: may write rows, may not change structure.
	GRANT USAGE ON SCHEMA raw TO airspace_ingest;
	GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA raw TO airspace_ingest;
	ALTER DEFAULT PRIVILEGES FOR ROLE airspace_owner IN SCHEMA raw
	  GRANT SELECT, INSERT ON TABLES TO airspace_ingest;
	ALTER DEFAULT PRIVILEGES FOR ROLE airspace_owner IN SCHEMA raw
	  GRANT USAGE, SELECT ON SEQUENCES TO airspace_ingest;

	-- transform (dbt): reads raw, owns everything it builds downstream.
	GRANT USAGE ON SCHEMA raw TO airspace_transform;
	GRANT SELECT ON ALL TABLES IN SCHEMA raw TO airspace_transform;
	ALTER DEFAULT PRIVILEGES FOR ROLE airspace_owner IN SCHEMA raw
	  GRANT SELECT ON TABLES TO airspace_transform;
	SELECT format('GRANT CREATE ON DATABASE %I TO airspace_transform', :'db')\gexec

	-- reader: no grants yet. Marts do not exist until Phase 2; the role is
	-- created now so that no downstream component is ever tempted to read
	-- with a privileged account.

ESQL

echo "[init] roles and raw schema ready"
