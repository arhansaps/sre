#!/usr/bin/env python3
"""
run_chaos_pipeline.py — One-shot end-to-end data generation for agent testing.

Spikes payment failures a few times (so Alertmanager fires HighPaymentFailureRate
repeatedly), starts the producer (webhook receiver) and consumer in the background
just long enough to drain the resulting alerts into Kafka -> TimescaleDB/Pinecone,
then shuts everything down.

After this finishes, run `python -m agents.test` to test the RCA agent against
fresh data.

Usage:
  python scripts/run_chaos_pipeline.py
"""

from __future__ import annotations

import os
import subprocess
import sys
import time

import httpx

PRODUCER_HEALTH = "http://localhost:8080/health"

_ENV = {**os.environ, "PYTHONIOENCODING": "utf-8"}


def run(cmd: list[str]) -> None:
    print(f"$ {' '.join(cmd)}")
    subprocess.run(cmd, check=True, env=_ENV)


def wait_for_producer(timeout: int = 30) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = httpx.get(PRODUCER_HEALTH, timeout=2.0)
            if r.status_code == 200:
                return True
        except Exception:
            pass
        time.sleep(1)
    return False


def main() -> int:
    print("\n=== 0/4: Resetting any stale alert state ===")
    run([sys.executable, "scripts/chaos.py", "recover"])
    time.sleep(5)

    print("\n=== 1/4: Spiking payment-service failures + generating load (5x) ===")
    for i in range(5):
        print(f"\n-- spike {i + 1}/5 --")
        run([sys.executable, "scripts/chaos.py", "spike-payments"])
        run([sys.executable, "scripts/chaos.py", "load"])
        time.sleep(2)

    print("\n=== 2/4: Starting producer (webhook receiver) ===")
    producer = subprocess.Popen(
        [sys.executable, "-m", "pipeline.producer"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=_ENV,
    )
    if not wait_for_producer():
        producer.terminate()
        print("Producer failed to start (health check timed out).")
        return 1
    print("  -> producer healthy on :8080")

    print("\n=== 3/4: Starting consumer ===")
    consumer = subprocess.Popen(
        [sys.executable, "-m", "pipeline.consumer"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=_ENV,
    )

    print("\n=== 4/4: Sustaining load while waiting for Alertmanager to fire ===")
    print("  (HighPaymentFailureRate needs ~30-60s of sustained failures to fire)")
    for _ in range(6):
        run([sys.executable, "scripts/chaos.py", "load"])
    print("  -> waiting 60s for the alert + webhook + pipeline to drain")
    print("     (consumer loads a SentenceTransformer model on first message, which is slow)")
    time.sleep(60)

    print("\n=== Recovering services and shutting down producer/consumer ===")
    run([sys.executable, "scripts/chaos.py", "recover"])

    consumer.terminate()
    producer.terminate()
    try:
        consumer.wait(timeout=10)
    except subprocess.TimeoutExpired:
        consumer.kill()
    try:
        producer.wait(timeout=10)
    except subprocess.TimeoutExpired:
        producer.kill()

    print("\nDone. Run `python -m agents.test` to test the RCA agent against fresh data.")
    print("Run `python scripts/verify_pipeline.py` to confirm data landed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
