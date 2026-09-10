# Airflow image with the ingestion package baked in.
#
# The project is installed rather than bind-mounted so that what the scheduler
# runs is the same artefact CI built - a mounted source tree would let the
# container drift from the commit. Only ./dags is mounted, because DAG files
# are the thing you iterate on; changing ingestion code means rebuilding, which
# is the intended friction.
FROM apache/airflow:3.3.1-python3.12

# pyproject.toml is the single source of truth for runtime dependencies; they
# are not restated here.
COPY --chown=airflow:0 pyproject.toml /opt/airspace/pyproject.toml
COPY --chown=airflow:0 ingestion /opt/airspace/ingestion
COPY --chown=airflow:0 db /opt/airspace/db

RUN pip install --no-cache-dir /opt/airspace

# site-packages has no sibling db/ directory, so point the migration runner at
# the copy that came with the source.
ENV AIRSPACE_MIGRATIONS_DIR=/opt/airspace/db/migrations
