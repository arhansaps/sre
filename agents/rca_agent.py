"""
RCA Agent — root cause analysis using a LangGraph ReAct loop.

Flow:
  NormalisedEvent
      → [agent node]  LLM reasons, decides which tool to call
      → [tools node]  query TimescaleDB or search Pinecone
      → [agent node]  LLM sees results, reasons again
      → ... (loop until confident or AGENT_MAX_ITERATIONS hit)
      → [parse node]  extract structured RCAResult from final message
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import Annotated, Literal

from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, SystemMessage, ToolMessage
from langchain_core.tools import tool
from langgraph.graph import END, StateGraph
from langgraph.graph.message import add_messages
from pydantic import BaseModel
from typing_extensions import TypedDict

from pinecone import Pinecone
from sentence_transformers import SentenceTransformer

from agents.config import AGENT_MAX_ITERATIONS
from agents.llm import get_chat_model
from pipeline.config import EMBEDDING_MODEL, PINECONE_API_KEY, PINECONE_INDEX_NAME
from pipeline.db import get_connection
from pipeline.schema import NormalisedEvent

logger = logging.getLogger(__name__)


# ── Output schema ──────────────────────────────────────────────────────────────

class RCAResult(BaseModel):
    event_id: str
    service: str
    alert_name: str
    root_cause: str
    confidence: Literal["high", "medium", "low"]
    contributing_factors: list[str]
    suggested_fix: str
    similar_incident_ids: list[str]


# ── Graph state ────────────────────────────────────────────────────────────────

class RCAState(TypedDict):
    messages: Annotated[list[BaseMessage], add_messages]
    event: dict          # NormalisedEvent serialized — read-only context for nodes
    iterations: int
    rca: dict | None
    tool_call_log: list[str]   # signatures of tool calls already executed, for dedup


# ── Tools ──────────────────────────────────────────────────────────────────────

@tool
def query_recent_metrics(service: str, minutes: int = 30) -> str:
    """
    Query TimescaleDB for recent metric events for a service.
    Returns a JSON array of {time, metric_name, value} rows, newest first.
    Use this to see which metrics spiked around the incident time.
    """
    since = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT time, metric_name, value
                FROM metrics
                WHERE service = %s AND time >= %s
                ORDER BY time DESC
                LIMIT 100
                """,
                (service, since),
            )
            rows = [
                {"time": r[0].isoformat(), "metric_name": r[1], "value": r[2]}
                for r in cur.fetchall()
            ]
    if not rows:
        return json.dumps([{"note": f"No metrics found for {service} in the last {minutes} minutes"}])
    return json.dumps(rows)


@tool
def search_similar_incidents(description: str, top_k: int = 5) -> str:
    """
    Search Pinecone for past incidents semantically similar to the description.
    Returns a JSON array of past incidents with similarity scores and metadata.
    Use this to find historical precedent and known fixes.
    """
    
    encoder = SentenceTransformer(EMBEDDING_MODEL)
    vector = encoder.encode(description).tolist()

    pc = Pinecone(api_key=PINECONE_API_KEY)
    index = pc.Index(PINECONE_INDEX_NAME)
    results = index.query(vector=vector, top_k=top_k, include_metadata=True)

    matches = [
        {"event_id": m.id, "similarity_score": round(m.score, 3), **m.metadata}
        for m in results.matches
    ]
    if not matches:
        return json.dumps([{"note": "No similar incidents found in history"}])
    return json.dumps(matches)


#whatever outputs recieved from querying:
TOOLS = [query_recent_metrics, search_similar_incidents]


# ── System prompt ──────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """\
You are an expert SRE (Site Reliability Engineer) performing root cause analysis \
for a production incident. Be systematic and evidence-based.

You have two tools:
- query_recent_metrics(service, minutes): fetch recent time-series data from TimescaleDB
- search_similar_incidents(description, top_k): search incident history in Pinecone

Investigation strategy:
1. Query metrics for the affected service to see what spiked
2. Search for similar past incidents to find historical precedent
3. If the data implicates a dependency (e.g. downstream service), query its metrics too
4. Stop when you have enough evidence to explain the root cause with confidence

Rules:
- Never call a tool with the exact same name and arguments more than once — repeating an
  identical call will not return new information and wastes your limited iterations.
- Aim to reach a conclusion within 4-6 tool calls total. If the evidence is inconclusive
  after that, output your best-effort JSON with "confidence": "low" rather than continuing
  to investigate.

When you have finished your investigation, output ONLY a JSON object — no other text:
{
  "root_cause": "one clear sentence describing what caused the incident",
  "confidence": "high" | "medium" | "low",
  "contributing_factors": ["factor 1", "factor 2"],
  "suggested_fix": "concrete actionable remediation step",
  "similar_incident_ids": ["uuid1", "uuid2"]
}"""


# ── Bound model (cached so bind_tools only runs once) ─────────────────────────

@lru_cache(maxsize=1)
def _get_bound_model():
    return get_chat_model().bind_tools(TOOLS)


# ── Graph nodes ────────────────────────────────────────────────────────────────

_TOOLS_BY_NAME = {t.name: t for t in TOOLS}


def agent_node(state: RCAState) -> dict:
    response = _get_bound_model().invoke(state["messages"])
    return {
        "messages": [response],
        "iterations": state["iterations"] + 1,
    }


def _tool_call_signature(name: str, args: dict) -> str:
    return f"{name}({json.dumps(args, sort_keys=True)})"


def tool_node(state: RCAState) -> dict:
    """Execute pending tool calls, skipping any that exactly repeat a prior call."""
    last = state["messages"][-1]
    seen = set(state.get("tool_call_log", []))
    new_signatures = []
    tool_messages: list[ToolMessage] = []

    for call in last.tool_calls:
        signature = _tool_call_signature(call["name"], call["args"])
        if signature in seen:
            tool_messages.append(ToolMessage(
                content=(
                    f"[duplicate skipped] You already called {signature} earlier — "
                    "the result was unchanged. Try different arguments or finalize "
                    "your JSON answer now."
                ),
                tool_call_id=call["id"],
            ))
            continue

        result = _TOOLS_BY_NAME[call["name"]].invoke(call["args"])
        tool_messages.append(ToolMessage(content=str(result), tool_call_id=call["id"]))
        new_signatures.append(signature)

    return {
        "messages": tool_messages,
        "tool_call_log": [*seen, *new_signatures],
    }


def parse_node(state: RCAState) -> dict:
    """Pull the JSON RCA out of the last AI message."""
    last = state["messages"][-1]
    content = last.content if isinstance(last, AIMessage) else ""
    try:
        text = content.strip()
        # Strip markdown code fences if the model wrapped the JSON
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        rca = json.loads(text.strip())
    except (json.JSONDecodeError, IndexError):
        logger.error("Failed to parse RCA JSON: %s", content)
        rca = {
            "root_cause": content or "Could not determine root cause",
            "confidence": "low",
            "contributing_factors": [],
            "suggested_fix": "Manual investigation required",
            "similar_incident_ids": [],
        }
    return {"rca": rca}


def should_continue(state: RCAState) -> str:
    if state["iterations"] >= AGENT_MAX_ITERATIONS:
        logger.warning("RCA agent hit max iterations (%d), forcing output", AGENT_MAX_ITERATIONS)
        return "end"
    last = state["messages"][-1]
    if isinstance(last, AIMessage) and last.tool_calls:
        return "tools"
    return "end"


# ── Build GRaph ─────────────────────────────────────────────────────────────

@lru_cache(maxsize=1)
def _get_graph():
    graph = StateGraph(RCAState)

    graph.add_node("agent", agent_node)
    graph.add_node("tools", tool_node)
    graph.add_node("parse", parse_node)

    graph.set_entry_point("agent")
    graph.add_conditional_edges("agent", should_continue, {"tools": "tools", "end": "parse"})
    graph.add_edge("tools", "agent")
    graph.add_edge("parse", END)

    return graph.compile()


# ── Public API ─────────────────────────────────────────────────────────────────

def run_rca(event: NormalisedEvent) -> RCAResult:
    """Run root cause analysis for a normalised incident event."""
    logger.info(
        "RCA started event_id=%s service=%s alert=%s",
        event.event_id, event.service, event.alert_name,
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

    rca = final_state.get("rca") or {}
    result = RCAResult(
        event_id=event.event_id,
        service=event.service,
        alert_name=event.alert_name,
        root_cause=rca.get("root_cause", "Unknown"),
        confidence=rca.get("confidence", "low"),
        contributing_factors=rca.get("contributing_factors", []),
        suggested_fix=rca.get("suggested_fix", "Manual investigation required"),
        similar_incident_ids=rca.get("similar_incident_ids", []),
    )

    logger.info(
        "RCA complete event_id=%s confidence=%s root_cause=%s",
        result.event_id, result.confidence, result.root_cause,
    )
    return result
