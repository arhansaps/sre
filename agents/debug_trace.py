import sys
sys.stdout.reconfigure(encoding="utf-8")

import json
from datetime import datetime, timezone

from langchain_core.messages import AIMessage, ToolMessage, SystemMessage, HumanMessage

from agents.rca_agent import _get_graph, SYSTEM_PROMPT, check_service_config
from pipeline.schema import NormalisedEvent

# First, sanity-check the tool directly (bypasses the LLM entirely)
print("=== Direct check_service_config calls ===")
for service in ("payment-service", "order-service", "inventory-service"):
    print(f"{service}: {check_service_config.invoke({'service': service})}")

event = NormalisedEvent(
    timestamp=datetime.now(timezone.utc),
    source="prometheus",
    severity="critical",
    service="payment-service",
    alert_name="HighPaymentFailureRate",
    description="Payment failure rate spike detected",
    labels={"severity": "critical"},
)

initial_message = HumanMessage(content=(
    f"Investigate this production incident and determine the root cause.\n\n"
    f"Event ID: {event.event_id}\n"
    f"Service:  {event.service}\n"
    f"Alert:    {event.alert_name}\n"
    f"Severity: {event.severity}\n"
    f"Time:     {event.timestamp.isoformat()}\n"
    f"Description: {event.description}\n"
    f"Labels: {json.dumps(event.labels)}"
))

final_state = _get_graph().invoke({
    "messages": [SystemMessage(content=SYSTEM_PROMPT), initial_message],
    "event": event.model_dump(mode="json"),
    "iterations": 0,
    "rca": None,
    "tool_call_log": [],
})

print("\n=== Message trace ===")
for i, msg in enumerate(final_state["messages"]):
    if isinstance(msg, AIMessage):
        if msg.tool_calls:
            for call in msg.tool_calls:
                print(f"[{i}] AI -> tool call: {call['name']}({call['args']})")
        else:
            print(f"[{i}] AI -> final content: {msg.content[:300]}")
    elif isinstance(msg, ToolMessage):
        print(f"[{i}] TOOL result: {str(msg.content)[:300]}")
    else:
        print(f"[{i}] {type(msg).__name__}: {str(msg.content)[:200]}")
