#!/usr/bin/env python3
"""
Send a synthetic Prometheus Alertmanager webhook to the pipeline producer.

Use this to verify the pipeline without waiting for real Prometheus alerts.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone

import httpx

WEBHOOK_URL = "http://localhost:8080/webhook/prometheus"


def sample_payload() -> dict:
    """Minimal Alertmanager v4 payload mimicking HighPaymentFailureRate."""
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    return {
        "version": "4",
        "groupKey": '{}/{service="payment-service"}:{alertname="HighPaymentFailureRate"}',
        "status": "firing",
        "receiver": "incident-autopilot",
        "alerts": [
            {
                "status": "firing",
                "labels": {
                    "alertname": "HighPaymentFailureRate",
                    "severity": "critical",
                    "service": "payment-service",
                },
                "annotations": {
                    "summary": "Payment failure rate spike detected",
                    "description": "Payment failures > 10% over last 5 min (test injection).",
                },
                "startsAt": now,
                "endsAt": "0001-01-01T00:00:00Z",
                "fingerprint": "test-high-payment-failure",
            }
        ],
    }


def main() -> int:
    url = sys.argv[1] if len(sys.argv) > 1 else WEBHOOK_URL
    payload = sample_payload()
    print(f"POST {url}")
    try:
        r = httpx.post(url, json=payload, timeout=10.0)
        r.raise_for_status()
    except Exception as exc:
        print(f"Failed: {exc}")
        print("Is the producer running? python -m pipeline.producer")
        return 1
    print(json.dumps(r.json(), indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
