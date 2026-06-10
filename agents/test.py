import sys
sys.stdout.reconfigure(encoding="utf-8")

from pipeline.db import get_connection
from pipeline.schema import NormalisedEvent
from agents.rca_agent import run_rca
from datetime import datetime, timezone

event = NormalisedEvent(
    timestamp=datetime.now(timezone.utc),
    source="prometheus",
    severity="critical",
    service="payment-service",
    alert_name="HighPaymentFailureRate",
    description="Payment failure rate spike detected",
    labels={"severity": "critical"},
)
result = run_rca(event)
print(result.model_dump_json(indent=2))
