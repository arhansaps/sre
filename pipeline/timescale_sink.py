#writes data to postgre along with timestamp to query later

"""
TimescaleDB sink: persist normalised incident events as metrics and incident rows.
"""

from __future__ import annotations

import logging

from pipeline.db import get_connection, insert_metric, upsert_incident_firing
from pipeline.schema import NormalisedEvent

logger = logging.getLogger(__name__)


def write_event(event: NormalisedEvent) -> None:
    """
    Write one normalised event to TimescaleDB.

    Stores a metric sample (value 1.0 for firing) and upserts the incidents table.
    """
    alert_status = event.raw.get("alert", {}).get("status", "firing")
    metric_value = 1.0 if alert_status == "firing" else 0.0

    with get_connection() as conn:
        insert_metric(
            conn,
            time=event.timestamp,
            service=event.service,
            metric_name=event.alert_name,
            value=metric_value,
            labels={
                **event.labels,
                "severity": event.severity,
                "source": event.source,
                "event_id": event.event_id,
            },
        )
        if alert_status == "firing":
            upsert_incident_firing(
                conn,
                event_id=event.event_id,
                started_at=event.timestamp,
                severity=event.severity,
                service=event.service,
                alert_name=event.alert_name,
                raw_payload=event.model_dump(mode="json"),
            )

    logger.info(
        "timescale_sink wrote event_id=%s service=%s alert=%s",
        event.event_id,
        event.service,
        event.alert_name,
    )
