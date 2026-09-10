# Read API and dashboard.
#
# Runs as a non-root user and is given only the reader credential, so a
# compromise of this container yields access to aggregate marts and nothing
# else - not the landing zone, not the ability to write anything.
FROM python:3.12-slim

# Dependencies come from pyproject so there is one source of truth.
WORKDIR /opt/airspace
COPY pyproject.toml ./
COPY ingestion ./ingestion
COPY api ./api

RUN pip install --no-cache-dir ".[api]" \
    && useradd --system --no-create-home --uid 10001 airspace \
    && chown -R airspace:airspace /opt/airspace

USER airspace

EXPOSE 8000

# python rather than curl: the slim image has no curl, and adding one just for
# a healthcheck would widen the image for nothing.
HEALTHCHECK --interval=15s --timeout=5s --start-period=15s --retries=5 \
    CMD ["python", "-c", "import sys,urllib.request; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=3).status == 200 else 1)"]

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000", "--no-server-header"]
