"""
Kafka consumer: read raw alerts, normalise, fan out to sinks and `incidents.normalised`.
"""

from __future__ import annotations

import json
import logging
import signal
import sys
from typing import Any

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

from pipeline import pinecone_sink, timescale_sink
from pipeline.config import (
    CONSUMER_GROUP,
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_NORMALISED,
    TOPIC_RAW,
    require_timescale,
)
from pipeline.schema import AlertmanagerAlert, AlertmanagerWebhook, NormalisedEvent, normalise_prometheus_alert

logging.basicConfig(
    level=logging.INFO,
    format='{"level":"%(levelname)s","logger":"%(name)s","message":"%(message)s"}',
)
logger = logging.getLogger(__name__)

_running = True


def _handle_signal(*_args: Any) -> None:
    global _running
    _running = False
    logger.info("Shutdown requested")


def _build_consumer() -> KafkaConsumer:
    """Subscribe to raw incident topic."""
    return KafkaConsumer(
        TOPIC_RAW,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )


def _build_producer() -> KafkaProducer:
    """Publish normalised events downstream."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: v if isinstance(v, bytes) else v,
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
    )


def normalise_raw_envelope(payload: dict[str, Any]) -> NormalisedEvent | None:
    """
    Convert a raw Kafka envelope to NormalisedEvent.

    Supports Alertmanager-shaped payloads from the producer webhook.
    """
    source = payload.get("source", "prometheus")
    if source != "prometheus":
        logger.warning("Unsupported source: %s", source)
        return None

    alert_data = payload.get("alert")
    if not alert_data:
        logger.warning("Missing alert in payload")
        return None

    alert = AlertmanagerAlert.model_validate(alert_data)
    group_status = payload.get("webhook_status", alert.status)
    return normalise_prometheus_alert(alert, group_status=group_status)


def process_message(payload: dict[str, Any], producer: KafkaProducer) -> None:
    """Normalise one raw message and write to all sinks."""
    event = normalise_raw_envelope(payload)
    if event is None:
        return

    logger.info(
        json.dumps(
            {
                "type": "event_normalised",
                "event_id": event.event_id,
                "service": event.service,
                "alert_name": event.alert_name,
            }
        )
    )

    try:
        timescale_sink.write_event(event)
    except Exception as exc:
        logger.error("TimescaleDB sink failed: %s", exc)

    try:
        pinecone_sink.write_event(event)
    except Exception as exc:
        logger.error("Pinecone sink failed: %s", exc)

    try:
        producer.send(
            TOPIC_NORMALISED,
            value=event.to_kafka_bytes(),
            key=event.service.encode("utf-8"),
        ).get(timeout=10)
    except KafkaError as exc:
        logger.error("Failed to publish to %s: %s", TOPIC_NORMALISED, exc)


def run_consumer() -> None:
    """Main consume loop until SIGINT/SIGTERM."""
    require_timescale()

    signal.signal(signal.SIGINT, _handle_signal)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _handle_signal)

    consumer = _build_consumer()
    producer = _build_producer()
    logger.info("Consumer started: topic=%s group=%s", TOPIC_RAW, CONSUMER_GROUP)

    try:
        while _running:
            records = consumer.poll(timeout_ms=1000, max_records=50)
            for _tp, messages in records.items():
                for msg in messages:
                    try:
                        process_message(msg.value, producer)
                    except Exception as exc:
                        logger.exception("Message processing failed: %s", exc)
    finally:
        producer.flush()
        producer.close()
        consumer.close()
        logger.info("Consumer stopped")


def main() -> None:
    """CLI entry point."""
    try:
        run_consumer()
    except KeyboardInterrupt:
        pass
    sys.exit(0)


if __name__ == "__main__":
    main()
