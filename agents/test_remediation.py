import sys
sys.stdout.reconfigure(encoding="utf-8")

import time
from datetime import datetime, timezone

import httpx

from agents.rca_agent import run_rca
from agents.remediation_agent import run_remediation
from pipeline.schema import NormalisedEvent
from scripts.chaos import set_env

SERVICE = "payment-service"
PORT = 8002


def get_config(retries: int = 10, delay: float = 1.0) -> dict:
    """Container may still be restarting after set_env's force-recreate; retry briefly."""
    last_error = None
    for _ in range(retries):
        try:
            return httpx.get(f"http://localhost:{PORT}/debug/config", timeout=5.0).json()
        except httpx.TransportError as e:
            last_error = e
            time.sleep(delay)
    raise last_error


print("=== Injecting chaos: payment-service FAILURE_RATE=80% ===")
set_env(SERVICE, failure_rate=80, latency_ms=0)

config_before = get_config()
print(f"Config before remediation: {config_before}")

event = NormalisedEvent(
    timestamp=datetime.now(timezone.utc),
    source="prometheus",
    severity="critical",
    service=SERVICE,
    alert_name="HighPaymentFailureRate",
    description="Payment failure rate spike detected",
    labels={"severity": "critical"},
)

rca = run_rca(event)
print("\n=== RCA ===")
print(rca.model_dump_json(indent=2))

remediation = run_remediation(rca)
print("\n=== Remediation ===")
print(remediation.model_dump_json(indent=2))

config_after = get_config()
print(f"\nConfig after remediation: {config_after}")

if config_after.get("failure_rate") == 0:
    print("\n✓ payment-service was healed (FAILURE_RATE reset to 0)")
else:
    print("\n✗ payment-service still unhealthy — remediation did not fix it")
