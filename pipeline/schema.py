"""
Pydantic models for the incident ingestion pipeline.

Defines the canonical event shape consumed by LangGraph agents in later weeks.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, Field, field_validator

SourceType = Literal["prometheus", "cloudwatch", "pagerduty"]
SeverityType = Literal["critical", "warning", "info"]


class NormalisedEvent(BaseModel):
    """Canonical incident signal after normalisation from any alert source."""

    event_id: str = Field(default_factory=lambda: str(uuid4()))
    timestamp: datetime
    source: SourceType
    severity: SeverityType
    service: str
    alert_name: str
    description: str
    labels: dict[str, Any] = Field(default_factory=dict)
    raw: dict[str, Any] = Field(default_factory=dict)

    @field_validator("timestamp", mode="before")
    @classmethod
    def ensure_utc(cls, value: datetime | str) -> datetime:
        """Parse ISO strings and normalise naive datetimes to UTC."""
        if isinstance(value, str):
            value = value.replace("Z", "+00:00")
            parsed = datetime.fromisoformat(value)
        else:
            parsed = value
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    def to_kafka_bytes(self) -> bytes:
        """Serialize for Kafka value payload."""
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def from_kafka_bytes(cls, data: bytes) -> NormalisedEvent:
        """Deserialize from Kafka consumer."""
        return cls.model_validate_json(data)


class AlertmanagerAlert(BaseModel):
    """Single alert inside an Alertmanager webhook payload."""

    status: str
    labels: dict[str, str] = Field(default_factory=dict)
    annotations: dict[str, str] = Field(default_factory=dict)
    startsAt: str = ""
    endsAt: str = ""
    generatorURL: str = ""
    fingerprint: str = ""


class AlertmanagerWebhook(BaseModel):
    """Prometheus Alertmanager v4 webhook body."""

    version: str = "4"
    groupKey: str = ""
    status: str = "firing"
    receiver: str = ""
    groupLabels: dict[str, str] = Field(default_factory=dict)
    commonLabels: dict[str, str] = Field(default_factory=dict)
    commonAnnotations: dict[str, str] = Field(default_factory=dict)
    externalURL: str = ""
    alerts: list[AlertmanagerAlert] = Field(default_factory=list)


def normalise_prometheus_alert(
    alert: AlertmanagerAlert,
    *,
    group_status: str = "firing",
    webhook: AlertmanagerWebhook | None = None,
) -> NormalisedEvent:
    """
    Map an Alertmanager alert to our canonical schema.

    Preserves the full alert and optional webhook envelope in `raw` for audit.
    """
    labels = dict(alert.labels)
    annotations = dict(alert.annotations)

    severity_raw = labels.get("severity", "warning").lower()
    if severity_raw not in ("critical", "warning", "info"):
        severity_raw = "warning"

    service = labels.get("service") or labels.get("job") or "unknown"
    alert_name = labels.get("alertname", "UnknownAlert")
    description = (
        annotations.get("description")
        or annotations.get("summary")
        or f"Alert {alert_name} on {service}"
    )

    raw: dict[str, Any] = {
        "alert": alert.model_dump(),
        "group_status": group_status,
    }
    if webhook is not None:
        raw["webhook"] = {
            "receiver": webhook.receiver,
            "groupKey": webhook.groupKey,
            "commonLabels": webhook.commonLabels,
        }

    ts_str = alert.startsAt or ""
    timestamp = datetime.now(timezone.utc)
    if ts_str:
        try:
            ts_str_norm = ts_str.replace("Z", "+00:00")
            parsed = datetime.fromisoformat(ts_str_norm)
            if parsed.tzinfo is None:
                timestamp = parsed.replace(tzinfo=timezone.utc)
            else:
                timestamp = parsed.astimezone(timezone.utc)
        except ValueError:
            pass

    return NormalisedEvent(
        timestamp=timestamp,
        source="prometheus",
        severity=severity_raw,  # type: ignore[arg-type]
        service=service,
        alert_name=alert_name,
        description=description,
        labels=labels,
        raw=raw,
    )
