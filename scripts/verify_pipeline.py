#!/usr/bin/env python3
"""
Verify week-2 pipeline: Kafka topics, TimescaleDB metrics, optional Pinecone stats.
"""

from __future__ import annotations

import json
import sys

from dotenv import load_dotenv

load_dotenv()


def check_kafka() -> bool:
    """Print message counts from raw and normalised topics."""
    from kafka import KafkaConsumer, TopicPartition

    from pipeline.config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_NORMALISED, TOPIC_RAW

    ok = True
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    for topic in (TOPIC_RAW, TOPIC_NORMALISED):
        parts = consumer.partitions_for_topic(topic)
        if not parts:
            print(f"  ✗ Kafka topic '{topic}': not found (no messages yet?)")
            ok = False
            continue
        total = 0
        for p in parts:
            tp = TopicPartition(topic, p)
            end = consumer.end_offsets([tp])[tp]
            begin = consumer.beginning_offsets([tp])[tp]
            total += end - begin
        print(f"  ✓ Kafka topic '{topic}': {total} message(s)")
        if total == 0:
            ok = False
    consumer.close()
    return ok


def check_timescale() -> bool:
    """Count recent rows in metrics table."""
    from pipeline.config import TIMESCALE_URL
    import psycopg2

    conn = psycopg2.connect(TIMESCALE_URL)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM metrics")
            total = cur.fetchone()[0]
            cur.execute(
                """
                SELECT service, metric_name, value, time
                FROM metrics ORDER BY time DESC LIMIT 5
                """
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    print(f"  ✓ TimescaleDB metrics rows: {total}")
    if total == 0:
        return False
    for row in rows:
        print(f"      {row[3]} | {row[0]} | {row[1]} = {row[2]}")
    return True


def check_pinecone() -> bool:
    """Print Pinecone vector count if configured."""
    from pipeline import pinecone_sink

    msg = pinecone_sink.describe_stats()
    print(f"  {'✓' if 'unavailable' not in msg.lower() else '○'} {msg}")
    return "unavailable" not in msg.lower()


def main() -> int:
    print("\n── Pipeline verification ───────────────────────")
    kafka_ok = check_kafka()
    ts_ok = check_timescale()
    pinecone_ok = check_pinecone()
    print()
    if kafka_ok and ts_ok:
        print("Core pipeline OK (Kafka + TimescaleDB).")
        if not pinecone_ok:
            print("Pinecone skipped or empty — set PINECONE_API_KEY in .env to enable.")
        return 0
    print("Pipeline incomplete — run producer, consumer, then send_test_alert.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
