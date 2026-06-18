"""
Supervisor — consumes normalised incident events and runs the agent pipeline.

Flow:
  Kafka (incidents.normalised)
      → [RCA agent]          diagnose root cause (TSDB metrics + Pinecone RAG + live config)
      → [Remediation agent]  pick and execute a whitelisted fix
      → [Postmortem agent]   generate blameless postmortem → Slack + S3
      → [TimescaleDB]        record root_cause + resolution on the incident row
"""

from __future__ import annotations

import json
import logging
import signal
import sys
from typing import Any

from kafka import KafkaConsumer
from kafka.errors import KafkaError

from agents.postmortem_agent import run_postmortem
from agents.rca_agent import run_rca
from agents.remediation_agent import run_remediation
from pipeline.config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_NORMALISED, require_timescale
from pipeline.db import get_connection, update_incident_result
from pipeline.schema import NormalisedEvent

logging.basicConfig(
    level=logging.INFO,
    format='{"level":"%(levelname)s","logger":"%(name)s","message":"%(message)s"}',
)
logger = logging.getLogger(__name__)

CONSUMER_GROUP = "incident-supervisor"

_running = True


def _handle_signal(*_args: Any) -> None:
    global _running
    _running = False
    logger.info("Shutdown requested")


def _build_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        TOPIC_NORMALISED,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda m: m,
    )


def handle_event(event: NormalisedEvent) -> None:
    """Run RCA + remediation for one incident and persist the outcome."""
    alert_status = event.raw.get("alert", {}).get("status", "firing")
    if alert_status != "firing":
        logger.info("Skipping resolved alert event_id=%s", event.event_id)
        return

    logger.info(
        json.dumps({
            "type": "supervisor_start",
            "event_id": event.event_id,
            "service": event.service,
            "alert_name": event.alert_name,
        })
    )

    rca = run_rca(event)
    remediation = run_remediation(rca)
    postmortem = run_postmortem(rca, remediation)

    print("\n" + "=" * 60)
    print("POSTMORTEM")
    print("=" * 60)
    print(postmortem.document)
    print("=" * 60 + "\n")

    with get_connection() as conn:
        update_incident_result(
            conn,
            event_id=event.event_id,
            root_cause=rca.root_cause,
            resolution=(
                f"{remediation.action}({remediation.params}) -> {remediation.outcome.get('detail', '')}"
            ),
            resolved=remediation.success,
        )

    logger.info(
        json.dumps({
            "type": "supervisor_done",
            "event_id": event.event_id,
            "confidence": rca.confidence,
            "action": remediation.action,
            "success": remediation.success,
            "s3_key": postmortem.s3_key,
            "slack_ts": postmortem.slack_ts,
        })
    )


def run_supervisor() -> None:
    """Main consume loop until SIGINT/SIGTERM."""
    require_timescale()

    signal.signal(signal.SIGINT, _handle_signal)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _handle_signal)

    consumer = _build_consumer()
    logger.info("Supervisor started: topic=%s group=%s", TOPIC_NORMALISED, CONSUMER_GROUP)

    try:
        while _running:
            records = consumer.poll(timeout_ms=1000, max_records=10)
            for _tp, messages in records.items():
                for msg in messages:
                    try:
                        event = NormalisedEvent.from_kafka_bytes(msg.value)
                        handle_event(event)
                    except KafkaError as exc:
                        logger.error("Kafka error: %s", exc)
                    except Exception:
                        logger.exception("Failed to process message")
    finally:
        consumer.close()
        logger.info("Supervisor stopped")


def main() -> None:
    try:
        run_supervisor()
    except KeyboardInterrupt:
        pass
    sys.exit(0)


if __name__ == "__main__":
    main()
