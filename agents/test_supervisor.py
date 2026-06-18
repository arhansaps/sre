"""
Publish one normalised incident event to Kafka so `agents.supervisor` picks it up.

Run `python -m agents.supervisor` in another terminal first (or after), then run this
script. The supervisor will run RCA + remediation and write the result back to the
`incidents` table.
"""

import sys
sys.stdout.reconfigure(encoding="utf-8")

from datetime import datetime, timezone

from kafka import KafkaProducer

from pipeline.config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_NORMALISED
from pipeline.db import get_connection, upsert_incident_firing
from pipeline.schema import NormalisedEvent
from scripts.chaos import set_env

SERVICE = "payment-service"

print("=== Injecting chaos: payment-service FAILURE_RATE=80% ===")
set_env(SERVICE, failure_rate=80, latency_ms=0)

event = NormalisedEvent(
    timestamp=datetime.now(timezone.utc),
    source="prometheus",
    severity="critical",
    service=SERVICE,
    alert_name="HighPaymentFailureRate",
    description="Payment failure rate spike detected",
    labels={"severity": "critical"},
    raw={"alert": {"status": "firing"}},
)

# The incidents row must exist before the supervisor updates it.
with get_connection() as conn:
    upsert_incident_firing(
        conn,
        event_id=event.event_id,
        started_at=event.timestamp,
        severity=event.severity,
        service=event.service,
        alert_name=event.alert_name,
        raw_payload=event.model_dump(mode="json"),
    )

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: v,
    key_serializer=lambda k: k.encode("utf-8") if k else None,
    acks="all",
)
producer.send(TOPIC_NORMALISED, value=event.to_kafka_bytes(), key=event.service).get(timeout=10)
producer.flush()
producer.close()

print(f"Published event_id={event.event_id} to {TOPIC_NORMALISED}")
print("Watch the supervisor's logs, then check the incidents table:")
print(f"  SELECT root_cause, resolution, resolved_at FROM incidents WHERE id = '{event.event_id}';")
