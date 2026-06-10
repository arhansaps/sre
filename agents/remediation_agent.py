"""
Remediation Agent — given an RCA result, decides and executes a safe,
whitelisted remediation action with audit logging.

Flow:
  RCAResult
      → [decide]   LLM picks one action from a fixed whitelist
      → [execute]  run the chosen Python function (no arbitrary commands)
      → [log]      record the decision + outcome to remediation_actions
"""

from __future__ import annotations

import json
import logging
from typing import Callable

from pydantic import BaseModel

from agents.llm import chat
from agents.rca_agent import RCAResult
from pipeline.db import get_connection, log_remediation_action
from scripts.chaos import set_env

logger = logging.getLogger(__name__)


# ── Output schema ──────────────────────────────────────────────────────────────

class RemediationResult(BaseModel):
    event_id: str
    action: str
    params: dict
    reasoning: str
    outcome: dict
    success: bool


# ── Whitelisted actions ──────────────────────────────────────────────────────────
# The LLM may only select one of these by name — it never executes arbitrary code.

def restart_service(service: str) -> dict:
    """Restart a service by recreating its container with healthy env vars."""
    set_env(service, failure_rate=0, latency_ms=0)
    return {"status": "success", "detail": f"{service} restarted with healthy config"}


def rollback_config(service: str) -> dict:
    """Roll a service back to its last known good configuration."""
    set_env(service, failure_rate=0, latency_ms=0)
    return {"status": "success", "detail": f"{service} configuration rolled back to last known good"}


def scale_service(service: str, replicas: int = 2) -> dict:
    """Simulate scaling a service (no real infra change in this lab setup)."""
    logger.info("Simulated scale: %s -> %d replicas", service, replicas)
    return {"status": "simulated", "detail": f"{service} scaled to {replicas} replicas (simulated)"}


def escalate_to_human(reason: str = "No safe automated action available") -> dict:
    """No automated action — flag the incident for human review."""
    logger.warning("Escalating to human: %s", reason)
    return {"status": "escalated", "detail": reason}


REMEDIATION_ACTIONS: dict[str, Callable[..., dict]] = {
    "restart_service": restart_service,
    "rollback_config": rollback_config,
    "scale_service": scale_service,
    "escalate_to_human": escalate_to_human,
}


# ── Decision prompt ──────────────────────────────────────────────────────────────

DECISION_PROMPT = """\
You are an SRE remediation planner. Given a root cause analysis, choose ONE \
action from this whitelist to remediate the incident:

- restart_service(service): restart the affected service's container
- rollback_config(service): roll back the service to its last known good configuration
- scale_service(service, replicas): scale the service horizontally (replicas: int)
- escalate_to_human(reason): no safe automated fix exists — flag for human review

Rules:
- Only choose escalate_to_human if confidence is "low" or no action clearly addresses the root cause.
- "service" params must be one of: order-service, payment-service, inventory-service.
- Output ONLY a JSON object, no other text:
{
  "action": "restart_service" | "rollback_config" | "scale_service" | "escalate_to_human",
  "params": {...},
  "reasoning": "one sentence explaining why this action addresses the root cause"
}"""


def _decide(rca: RCAResult) -> dict:
    response = chat([
        {"role": "system", "content": DECISION_PROMPT},
        {"role": "user", "content": rca.model_dump_json(indent=2)},
    ])
    text = response.strip()
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
    try:
        decision = json.loads(text.strip())
    except json.JSONDecodeError:
        logger.error("Failed to parse remediation decision: %s", response)
        decision = {
            "action": "escalate_to_human",
            "params": {"reason": "Could not parse remediation decision"},
            "reasoning": "Fallback after JSON parse failure",
        }
    return decision


# ── Public API ─────────────────────────────────────────────────────────────────

def run_remediation(rca: RCAResult) -> RemediationResult:
    """Decide on and execute a remediation action for the given RCA result."""
    decision = _decide(rca)
    action = decision.get("action", "")
    params = decision.get("params", {}) or {}
    reasoning = decision.get("reasoning", "")

    if action not in REMEDIATION_ACTIONS:
        logger.warning("Unknown remediation action %r, escalating", action)
        params = {"reason": f"Unknown action {action!r} chosen by model"}
        action = "escalate_to_human"

    try:
        outcome = REMEDIATION_ACTIONS[action](**params)
        success = outcome.get("status") in ("success", "simulated")
    except Exception as e:
        logger.exception("Remediation action %s failed", action)
        outcome = {"status": "error", "detail": str(e)}
        success = False

    result = RemediationResult(
        event_id=rca.event_id,
        action=action,
        params=params,
        reasoning=reasoning,
        outcome=outcome,
        success=success,
    )

    with get_connection() as conn:
        log_remediation_action(
            conn,
            event_id=result.event_id,
            service=rca.service,
            root_cause=rca.root_cause,
            action=result.action,
            params=result.params,
            reasoning=result.reasoning,
            outcome=result.outcome,
            success=result.success,
        )

    logger.info(
        "Remediation complete event_id=%s action=%s success=%s",
        result.event_id, result.action, result.success,
    )
    return result
