"""
TimescaleDB connection helpers and schema bootstrap.

Creates the metrics hypertable and incidents table used by the pipeline and agents.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Generator

import psycopg2
from psycopg2.extensions import connection as PgConnection
from psycopg2.extras import Json

from pipeline.config import TIMESCALE_URL

logger = logging.getLogger(__name__)

SCHEMA_SQL = """
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

CREATE TABLE IF NOT EXISTS metrics (
    time        TIMESTAMPTZ NOT NULL,
    service     TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    value       DOUBLE PRECISION,
    labels      JSONB
);

SELECT create_hypertable('metrics', 'time', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS incidents (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    started_at  TIMESTAMPTZ NOT NULL,
    resolved_at TIMESTAMPTZ,
    severity    TEXT,
    service     TEXT,
    alert_name  TEXT,
    root_cause  TEXT,
    resolution  TEXT,
    raw_payload JSONB
);

CREATE INDEX IF NOT EXISTS idx_metrics_service_time
    ON metrics (service, time DESC);

CREATE INDEX IF NOT EXISTS idx_incidents_service_started
    ON incidents (service, started_at DESC);
"""


def connect() -> PgConnection:
    """Open a new database connection using TIMESCALE_URL."""
    return psycopg2.connect(TIMESCALE_URL)


@contextmanager
def get_connection() -> Generator[PgConnection, None, None]:
    """Context manager that commits on success and rolls back on error."""
    conn = connect()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_schema() -> None:
    """
    Create TimescaleDB tables and hypertable if they do not exist.

    Safe to run multiple times (idempotent).
    """
    statements = [
        s.strip()
        for s in SCHEMA_SQL.split(";")
        if s.strip()
    ]
    with get_connection() as conn:
        with conn.cursor() as cur:
            for statement in statements:
                cur.execute(statement + ";")
    logger.info("TimescaleDB schema initialised")


def insert_metric(
    conn: PgConnection,
    *,
    time,
    service: str,
    metric_name: str,
    value: float,
    labels: dict | None = None,
) -> None:
    """Insert one row into the metrics hypertable."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO metrics (time, service, metric_name, value, labels)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (time, service, metric_name, value, Json(labels or {})),
        )


def upsert_incident_firing(
    conn: PgConnection,
    *,
    event_id: str,
    started_at,
    severity: str,
    service: str,
    alert_name: str,
    raw_payload: dict,
) -> None:
    """
    Record or refresh an open incident row when an alert fires.

    Uses event_id as a stable key via raw_payload for deduplication in week 2.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO incidents (
                id, started_at, severity, service, alert_name, raw_payload
            )
            VALUES (%s::uuid, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                severity = EXCLUDED.severity,
                raw_payload = EXCLUDED.raw_payload
            """,
            (event_id, started_at, severity, service, alert_name, Json(raw_payload)),
        )
