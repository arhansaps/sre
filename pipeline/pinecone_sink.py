#builds a text string from the alert, converts that text to a 384 number vector using the llm running locally
#upserts the vector to pinecone

"""
Pinecone sink: embed alert text locally and upsert vectors for semantic search.
"""

from __future__ import annotations

import logging
from typing import Any

from pipeline.config import EMBEDDING_MODEL, PINECONE_API_KEY, PINECONE_INDEX_NAME
from pipeline.schema import NormalisedEvent

logger = logging.getLogger(__name__)

_encoder = None
_index = None
_pinecone_unavailable_reason: str | None = None


def _embedding_text(event: NormalisedEvent) -> str:
    """Build searchable text from alert fields."""
    return (
        f"[{event.severity}] {event.alert_name} on {event.service}: "
        f"{event.description}"
    )


def _get_encoder():
    """Lazy-load sentence-transformers model (downloads on first use)."""
    global _encoder
    if _encoder is None:
        from sentence_transformers import SentenceTransformer

        logger.info("Loading embedding model: %s", EMBEDDING_MODEL)
        _encoder = SentenceTransformer(EMBEDDING_MODEL)
    return _encoder


def _get_index():
    """
    Lazy-connect to Pinecone index.

    Returns None if API key is missing or connection fails (non-fatal for pipeline).
    """
    global _index, _pinecone_unavailable_reason
    if _index is not None:
        return _index
    if _pinecone_unavailable_reason:
        return None

    if not PINECONE_API_KEY or PINECONE_API_KEY.startswith("..."):
        _pinecone_unavailable_reason = "PINECONE_API_KEY not configured"
        logger.warning("Pinecone sink disabled: %s", _pinecone_unavailable_reason)
        return None

    try:
        from pinecone import Pinecone, ServerlessSpec

        pc = Pinecone(api_key=PINECONE_API_KEY)
        if PINECONE_INDEX_NAME not in [idx.name for idx in pc.list_indexes()]:
            logger.info("Creating Pinecone index: %s", PINECONE_INDEX_NAME)
            pc.create_index(
                name=PINECONE_INDEX_NAME,
                dimension=384,
                metric="cosine",
                spec=ServerlessSpec(cloud="aws", region="us-east-1"),
            )
        _index = pc.Index(PINECONE_INDEX_NAME)
        return _index
    except Exception as exc:
        _pinecone_unavailable_reason = str(exc)
        logger.warning("Pinecone sink disabled: %s", exc)
        return None


def write_event(event: NormalisedEvent) -> None:
    """
    Embed and upsert one incident event into Pinecone.

    Skips gracefully when Pinecone is not configured (local dev without API key).
    """
    index = _get_index()
    if index is None:
        logger.debug("Skipping Pinecone upsert for event_id=%s", event.event_id)
        return

    encoder = _get_encoder()
    text = _embedding_text(event)
    vector = encoder.encode(text).tolist()

    metadata: dict[str, Any] = {
        "event_id": event.event_id,
        "service": event.service,
        "alert_name": event.alert_name,
        "severity": event.severity,
        "source": event.source,
        "description": event.description[:1000],
        "timestamp": event.timestamp.isoformat(),
    }

    index.upsert(vectors=[{"id": event.event_id, "values": vector, "metadata": metadata}])
    logger.info(
        "pinecone_sink upserted event_id=%s service=%s",
        event.event_id,
        event.service,
    )


def describe_stats() -> str:
    """Return human-readable Pinecone index stats for debugging."""
    index = _get_index()
    if index is None:
        return f"Pinecone unavailable: {_pinecone_unavailable_reason}"
    stats = index.describe_index_stats()
    return f"Pinecone index '{PINECONE_INDEX_NAME}': {stats.total_vector_count} vectors"
