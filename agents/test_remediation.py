import sys
sys.stdout.reconfigure(encoding="utf-8")

from datetime import datetime, timezone

from agents.rca_agent import run_rca
from agents.remediation_agent import run_remediation
from pipeline.schema import NormalisedEvent

event = NormalisedEvent(
    timestamp=datetime.now(timezone.utc),
    source="prometheus",
    severity="critical",
    service="payment-service",
    alert_name="HighPaymentFailureRate",
    description="Payment failure rate spike detected",
    labels={"severity": "critical"},
)

rca = run_rca(event)
print("=== RCA ===")
print(rca.model_dump_json(indent=2))

remediation = run_remediation(rca)
print("\n=== Remediation ===")
print(remediation.model_dump_json(indent=2))
