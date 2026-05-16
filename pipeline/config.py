"""
Shared environment configuration for the ingestion pipeline.

Validates required variables at import time so services fail fast with clear errors.
"""

from __future__ import annotations

import os
import sys

from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TIMESCALE_URL = os.getenv("TIMESCALE_URL", "postgresql://autopilot:autopilot@localhost:5432/incidents")

TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW", "incidents.raw")
TOPIC_NORMALISED = os.getenv("KAFKA_TOPIC_NORMALISED", "incidents.normalised")
TOPIC_RESOLVED = os.getenv("KAFKA_TOPIC_RESOLVED", "incidents.resolved")

PINECONE_API_KEY = os.getenv("PINECONE_API_KEY", "")
PINECONE_INDEX_NAME = os.getenv("PINECONE_INDEX_NAME", "incident-logs")
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "all-MiniLM-L6-v2")

WEBHOOK_HOST = os.getenv("WEBHOOK_HOST", "0.0.0.0")
WEBHOOK_PORT = int(os.getenv("WEBHOOK_PORT", "8080"))

CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "incident-pipeline")


def require_timescale() -> None:
    """Exit if TimescaleDB URL is missing (required for consumer and init_db)."""
    if not TIMESCALE_URL:
        print("ERROR: TIMESCALE_URL is required. Copy .env.example to .env", file=sys.stderr)
        sys.exit(1)
