"""
Prometheus Alertmanager webhook receiver → Kafka producer.

Receives firing/resolved alerts and publishes raw payloads to `incidents.raw`.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from kafka import KafkaProducer
from kafka.errors import KafkaError

from pipeline.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_RAW,
    WEBHOOK_HOST,
    WEBHOOK_PORT,
)
from pipeline.schema import AlertmanagerWebhook

logging.basicConfig(
    level=logging.INFO,
    format='{"level":"%(levelname)s","logger":"%(name)s","message":"%(message)s"}',
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Incident Pipeline Producer", version="0.1.0")

_producer: KafkaProducer | None = None


def get_kafka_producer() -> KafkaProducer:
    """Lazy singleton Kafka producer with JSON serialisation."""
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
            retries=3,
        )
    return _producer


def publish_raw(payload: dict[str, Any], *, key: str | None = None) -> None:
    """Publish one raw alert envelope to Kafka."""
    producer = get_kafka_producer()
    future = producer.send(TOPIC_RAW, value=payload, key=key)
    try:
        future.get(timeout=10)
    except KafkaError as exc:
        logger.error("Kafka publish failed: %s", exc)
        raise HTTPException(status_code=503, detail="Kafka unavailable") from exc


@app.get("/health")
async def health() -> dict[str, str]:
    """Liveness probe for docker / manual checks."""
    return {"status": "ok", "service": "pipeline-producer"}


@app.post("/webhook/prometheus")
async def prometheus_webhook(request: Request) -> dict[str, Any]:
    """
    Alertmanager webhook endpoint.

    Each alert in the payload is published individually to `incidents.raw`.
    """
    try:
        body = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid JSON body") from exc

    try:
        webhook = AlertmanagerWebhook.model_validate(body)
    except Exception as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    published = 0
    for alert in webhook.alerts:
        envelope = {
            "source": "prometheus",
            "webhook_status": webhook.status,
            "receiver": webhook.receiver,
            "alert": alert.model_dump(),
        }
        key = alert.fingerprint or alert.labels.get("alertname", "alert")
        publish_raw(envelope, key=key)
        published += 1
        logger.info(
            json.dumps(
                {
                    "type": "alert_published",
                    "topic": TOPIC_RAW,
                    "alert_name": alert.labels.get("alertname"),
                    "status": alert.status,
                }
            )
        )

    return {"status": "ok", "published": published, "topic": TOPIC_RAW}


@app.on_event("shutdown")
def shutdown_producer() -> None:
    """Flush and close Kafka producer on exit."""
    global _producer
    if _producer is not None:
        _producer.flush()
        _producer.close()
        _producer = None


def main() -> None:
    """Run uvicorn for local development."""
    import uvicorn

    uvicorn.run(
        "pipeline.producer:app",
        host=WEBHOOK_HOST,
        port=WEBHOOK_PORT,
        reload=False,
    )


if __name__ == "__main__":
    main()
