import sys
sys.stdout.reconfigure(encoding="utf-8")

import time
from datetime import datetime, timezone

import httpx

from agents.rca_agent import run_rca
from agents.remediation_agent import run_remediation
from pipeline.schema import NormalisedEvent
from scripts.chaos import set_env

SERVICE_PORTS = {
    "order-service": 8001,
    "payment-service": 8002,
    "inventory-service": 8003,
}

SCENARIOS = {
    "spike-payments": dict(
        service="payment-service",
        failure_rate=80,
        latency_ms=0,
        alert_name="HighPaymentFailureRate",
        description="Payment failure rate spike detected",
    ),
    "slow-inventory": dict(
        service="inventory-service",
        failure_rate=0,
        latency_ms=3000,
        alert_name="HighOrderLatency",
        description="Order request latency spike detected (inventory dependency slow)",
    ),
}


def get_config(port: int, retries: int = 10, delay: float = 1.0) -> dict:
    """Container may still be restarting after set_env's force-recreate; retry briefly."""
    last_error = None
    for _ in range(retries):
        try:
            return httpx.get(f"http://localhost:{port}/debug/config", timeout=5.0).json()
        except httpx.TransportError as e:
            last_error = e
            time.sleep(delay)
    raise last_error


scenario_name = sys.argv[1] if len(sys.argv) > 1 else "spike-payments"
if scenario_name not in SCENARIOS:
    print(f"Unknown scenario {scenario_name!r}. Available: {', '.join(SCENARIOS)}")
    sys.exit(1)

scenario = SCENARIOS[scenario_name]
service = scenario["service"]
port = SERVICE_PORTS[service]

print(f"=== Injecting chaos: {service} failure_rate={scenario['failure_rate']}% latency_ms={scenario['latency_ms']} ===")
set_env(service, failure_rate=scenario["failure_rate"], latency_ms=scenario["latency_ms"])

config_before = get_config(port)
print(f"Config before remediation: {config_before}")

event = NormalisedEvent(
    timestamp=datetime.now(timezone.utc),
    source="prometheus",
    severity="critical",
    service=service,
    alert_name=scenario["alert_name"],
    description=scenario["description"],
    labels={"severity": "critical"},
)

rca = run_rca(event)
print("\n=== RCA ===")
print(rca.model_dump_json(indent=2))

remediation = run_remediation(rca)
print("\n=== Remediation ===")
print(remediation.model_dump_json(indent=2))

config_after = get_config(port)
print(f"\nConfig after remediation: {config_after}")

if config_after.get("failure_rate") == 0 and config_after.get("latency_ms") == 0:
    print(f"\n✓ {service} was healed (config reset to healthy)")
else:
    print(f"\n✗ {service} still unhealthy — remediation did not fix it")
