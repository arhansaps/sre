"""
Postmortem Agent — given an RCA + remediation result, generates a blameless
postmortem document and publishes it to Slack and S3.

Flow:
  RCAResult + RemediationResult
      → [generate]  LLM writes a blameless postmortem in Markdown
      → [slack]     post summary to #incidents, full doc in thread
      → [s3]        upload Markdown to s3://{bucket}/postmortems/{event_id}.md
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from pydantic import BaseModel

from agents.config import AWS_REGION, S3_POSTMORTEM_BUCKET, SLACK_BOT_TOKEN, SLACK_INCIDENT_CHANNEL
from agents.llm import chat
from agents.rca_agent import RCAResult
from agents.remediation_agent import RemediationResult

logger = logging.getLogger(__name__)


# ── Output schema ──────────────────────────────────────────────────────────────

class PostmortemResult(BaseModel):
    event_id: str
    document: str       # full Markdown postmortem
    s3_key: str | None  # None if S3 not configured or upload failed
    slack_ts: str | None  # Slack message timestamp, None if Slack not configured


# ── Generation prompt ──────────────────────────────────────────────────────────

POSTMORTEM_PROMPT = """\
You are an SRE writing a blameless postmortem for a production incident.
Given the root cause analysis and remediation outcome below, write a clear,
concise postmortem in Markdown. Focus on systems and processes — never blame
individuals.

Use exactly this structure:

## Incident Summary
One paragraph: what happened, which service, when, how it was resolved.

## Timeline
Bullet list of key events with relative timestamps (T+0, T+1m, etc.).

## Root Cause
Clear technical explanation of what caused the incident.

## Impact
What was affected and for how long (estimate if exact data unavailable).

## What Went Well
2-3 bullets — things that helped limit impact or speed up resolution.

## What Could Be Improved
2-3 bullets — gaps in detection, response, or tooling.

## Action Items
Numbered list of concrete follow-up tasks with suggested owners (team, not person).

Output only the Markdown document — no preamble or commentary."""


def _generate(rca: RCAResult, remediation: RemediationResult) -> str:
    context = (
        f"### RCA Result\n{rca.model_dump_json(indent=2)}\n\n"
        f"### Remediation Result\n{remediation.model_dump_json(indent=2)}"
    )
    return chat([
        {"role": "system", "content": POSTMORTEM_PROMPT},
        {"role": "user", "content": context},
    ])


# ── Slack publisher ────────────────────────────────────────────────────────────

def _post_to_slack(event_id: str, rca: RCAResult, document: str) -> str | None:
    if not SLACK_BOT_TOKEN or SLACK_BOT_TOKEN.startswith("xoxb-..."):
        logger.warning("Slack not configured — skipping postmortem post")
        return None
    try:
        from slack_sdk import WebClient
        from slack_sdk.errors import SlackApiError

        client = WebClient(token=SLACK_BOT_TOKEN)

        summary = (
            f":fire: *Incident resolved* — `{rca.service}` / `{rca.alert_name}`\n"
            f"*Root cause:* {rca.root_cause}\n"
            f"*Confidence:* {rca.confidence} | *Event ID:* `{event_id}`\n"
            f"_Postmortem in thread_ :thread:"
        )
        main = client.chat_postMessage(channel=SLACK_INCIDENT_CHANNEL, text=summary)
        ts = main["ts"]

        client.chat_postMessage(
            channel=SLACK_INCIDENT_CHANNEL,
            thread_ts=ts,
            text=f"```\n{document[:3000]}\n```",
        )
        logger.info("Postmortem posted to Slack ts=%s", ts)
        return ts
    except SlackApiError as e:
        logger.error("Slack post failed: %s", e.response["error"])
        return None
    except Exception as e:
        logger.error("Slack post failed: %s", e)
        return None


# ── S3 uploader ────────────────────────────────────────────────────────────────

def _upload_to_s3(event_id: str, document: str) -> str | None:
    if not S3_POSTMORTEM_BUCKET:
        logger.warning("S3_POSTMORTEM_BUCKET not configured — skipping S3 upload")
        return None
    try:
        import boto3

        s3 = boto3.client("s3", region_name=AWS_REGION)
        key = f"postmortems/{event_id}.md"
        s3.put_object(
            Bucket=S3_POSTMORTEM_BUCKET,
            Key=key,
            Body=document.encode("utf-8"),
            ContentType="text/markdown",
        )
        logger.info("Postmortem uploaded to s3://%s/%s", S3_POSTMORTEM_BUCKET, key)
        return key
    except Exception as e:
        logger.error("S3 upload failed: %s", e)
        return None


# ── Public API ─────────────────────────────────────────────────────────────────

def run_postmortem(rca: RCAResult, remediation: RemediationResult) -> PostmortemResult:
    """Generate and publish a blameless postmortem for a resolved incident."""
    logger.info("Generating postmortem for event_id=%s", rca.event_id)

    document = _generate(rca, remediation)
    s3_key = _upload_to_s3(rca.event_id, document)
    slack_ts = _post_to_slack(rca.event_id, rca, document)

    logger.info(
        "Postmortem complete event_id=%s s3=%s slack_ts=%s",
        rca.event_id, s3_key, slack_ts,
    )
    return PostmortemResult(
        event_id=rca.event_id,
        document=document,
        s3_key=s3_key,
        slack_ts=slack_ts,
    )
