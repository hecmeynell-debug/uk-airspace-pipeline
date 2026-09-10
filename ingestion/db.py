"""Database connections and the migration runner.

Migrations are plain, numbered SQL files applied inside a transaction and
recorded with a checksum. The checksum matters: it turns "someone edited a
migration that has already run" from a silent divergence between environments
into a loud failure at startup.

Alembic would be the alternative. It is not used because the landing zone's
DDL is small, hand-written and reviewed, and dbt owns everything downstream -
so an ORM-oriented migration framework would add a dependency without adding
a guarantee.
"""

from __future__ import annotations

import hashlib
import os
from pathlib import Path

import psycopg

from ingestion.logging_setup import get_logger

log = get_logger(__name__)

# Defaults to the repo layout. Overridable because once the package is
# pip-installed - as it is in the Airflow image - site-packages has no sibling
# db/ directory, and silently finding no migrations would be worse than saying
# where to look.
MIGRATIONS_DIR = Path(
    os.environ.get(
        "AIRSPACE_MIGRATIONS_DIR",
        Path(__file__).resolve().parent.parent / "db" / "migrations",
    )
)

_MIGRATIONS_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS raw.schema_migrations (
    filename   text        PRIMARY KEY,
    checksum   text        NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT now()
)
"""


class MigrationError(RuntimeError):
    """A migration could not be applied, or has changed since it was applied."""


def connect(dsn: str) -> psycopg.Connection:
    return psycopg.connect(dsn)


def _checksum(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def discover_migrations(migrations_dir: Path | None = None) -> list[Path]:
    directory = migrations_dir or MIGRATIONS_DIR
    if not directory.is_dir():
        raise MigrationError(f"migrations directory not found: {directory}")
    return sorted(directory.glob("*.sql"))


def apply_migrations(conn: psycopg.Connection, migrations_dir: Path | None = None) -> list[str]:
    """Apply any unapplied migrations in filename order.

    Returns the filenames actually applied, so a caller can tell "already up to
    date" from "just changed the schema". Requires a connection with DDL rights
    on the raw schema - i.e. airspace_owner, never airspace_ingest.
    """
    applied: list[str] = []

    # Bookkeeping table and the read of it share one transaction that is
    # committed before the loop starts. Doing the read outside a transaction
    # block would leave an implicit one open, which would silently demote every
    # per-migration transaction below to a savepoint - so a failure in
    # migration N would roll back migrations 1..N-1 that had appeared to
    # succeed, and locks taken here would be held until the connection closed.
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(_MIGRATIONS_TABLE_DDL)
        cur.execute("SELECT filename, checksum FROM raw.schema_migrations")
        already_applied = dict(cur.fetchall())

    for path in discover_migrations(migrations_dir):
        body = path.read_text(encoding="utf-8")
        digest = _checksum(body)

        if path.name in already_applied:
            if already_applied[path.name] != digest:
                raise MigrationError(
                    f"{path.name} has changed since it was applied. Migrations are "
                    "immutable once run - add a new migration instead of editing this one."
                )
            log.debug("migration.skipped", filename=path.name)
            continue

        try:
            with conn.transaction():
                conn.execute(body)
                conn.execute(
                    "INSERT INTO raw.schema_migrations (filename, checksum) VALUES (%s, %s)",
                    (path.name, digest),
                )
        except psycopg.Error as exc:
            raise MigrationError(f"failed to apply {path.name}: {exc}") from exc

        log.info("migration.applied", filename=path.name)
        applied.append(path.name)

    if not applied:
        log.info("migration.up_to_date")

    return applied
