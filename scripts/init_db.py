#!/usr/bin/env python3
"""
One-time (idempotent) TimescaleDB schema initialisation.

Creates metrics hypertable and incidents table before running the pipeline consumer.
"""

from __future__ import annotations

import logging
import sys

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def main() -> int:
    """Run schema bootstrap and print confirmation."""
    from pipeline.config import require_timescale, TIMESCALE_URL
    from pipeline.db import init_schema

    require_timescale()
    logger.info("Connecting to %s", TIMESCALE_URL.split("@")[-1])

    try:
        init_schema()
    except Exception as exc:
        logger.error("Schema init failed: %s", exc)
        logger.error("Is TimescaleDB running? Try: docker compose up -d timescaledb")
        return 1

    logger.info("Schema ready: metrics (hypertable), incidents")
    return 0


if __name__ == "__main__":
    sys.exit(main())
