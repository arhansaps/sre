#!/usr/bin/env python3
"""
chaos.py — Inject failures into your fake microservices to test your agents.

Usage:
  python scripts/chaos.py spike-payments    # 80% payment failure rate
  python scripts/chaos.py slow-inventory    # 3s latency on inventory
  python scripts/chaos.py cascade           # payments fail → orders fail
  python scripts/chaos.py recover           # reset everything to healthy
  python scripts/chaos.py load              # send 50 orders to generate signal
  python scripts/chaos.py status            # show current health of all services
"""

import os
import sys
import time
import asyncio
import subprocess
import httpx


# the services are the docker containers
#mapped to their local URLs

SERVICES = {
    "order-service": "http://localhost:8001",
    "payment-service": "http://localhost:8002",
    "inventory-service": "http://localhost:8003",
}


def set_env(service: str, failure_rate: int = 0, latency_ms: int = 0):
    """Recreate (RESTART NOT RUNTIMEEE) a container with new env vars by using docker compose up --force-recreate."""
    env = {**os.environ, "FAILURE_RATE": str(failure_rate), "LATENCY_MS": str(latency_ms)}
    result = subprocess.run(
        ["docker", "compose", "up", "-d", "--no-deps", "--force-recreate", service],
        check=True,
        capture_output=True,
        env=env,
    )
    print(f"  ✓ {service}: failure_rate={failure_rate}%, latency={latency_ms}ms")


async def check_health(name: str, url: str) -> dict:
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            r = await client.get(f"{url}/health")
            return {"service": name, "status": "healthy", "code": r.status_code}
    except Exception as e:
        return {"service": name, "status": "unreachable", "error": str(e)}


async def send_orders(n: int = 20):
    """Fire n orders through the order service."""
    products = ["prod-001", "prod-002", "prod-003"]
    success, fail = 0, 0
    async with httpx.AsyncClient(timeout=10.0) as client:
        for i in range(n):
            try:
                r = await client.post("http://localhost:8001/orders", json={
                    "order_id": f"ord-{int(time.time())}-{i}",
                    "product_id": products[i % len(products)],
                    "quantity": 1,
                    "amount": round(10 + i * 2.5, 2),
                })
                if r.status_code == 200:
                    success += 1
                else:
                    fail += 1
            except Exception:
                fail += 1
            await asyncio.sleep(0.1)
    return success, fail


async def _check_all_services():
    """Gather health checks (asyncio.run needs a coroutine, not a Future)."""
    return await asyncio.gather(*[
        check_health(n, u) for n, u in SERVICES.items()
    ])


def cmd_status():
    print("\n── Service health ──────────────────────────────")
    results = asyncio.run(_check_all_services())
    for r in results:
        icon = "✓" if r["status"] == "healthy" else "✗"
        print(f"  {icon} {r['service']}: {r['status']}")
    print()



    # does nothing but recreates the docker containers wiht the new env vars to wreak havok lolz
    
def cmd_spike_payments():
    print("\n── Spiking payment failures to 80% ────────────")
    set_env("payment-service", failure_rate=80, latency_ms=0)
    print("  → Watch Prometheus: payment_failures_total should spike")
    print("  → Alert 'HighPaymentFailureRate' fires after ~1 min\n")


def cmd_slow_inventory():
    print("\n── Injecting 3s latency into inventory service ─")
    set_env("inventory-service", failure_rate=0, latency_ms=3000)
    print("  → Watch Prometheus: order_request_duration_seconds p95 will climb")
    print("  → Alert 'HighOrderLatency' fires after ~2 min\n")


def cmd_cascade():
    print("\n── Cascade failure: payment 90% fail + inventory slow ─")
    set_env("payment-service", failure_rate=90, latency_ms=500)
    set_env("inventory-service", failure_rate=30, latency_ms=1000)
    print("  → This simulates a realistic cascading incident")
    print("  → Multiple alerts will fire — great agent test case\n")


def cmd_recover():
    print("\n── Recovering all services ─────────────────────")
    for svc in ["payment-service", "inventory-service", "order-service"]:
        set_env(svc, failure_rate=0, latency_ms=0)
    print("  → All services back to healthy defaults\n")


async def cmd_load():
    print("\n── Sending 50 test orders ──────────────────────")
    s, f = await send_orders(50)
    print(f"  → Completed: {s} success, {f} failed")
    print(f"  → Success rate: {s/(s+f)*100:.1f}%\n")


COMMANDS = {
    "status": cmd_status,
    "spike-payments": cmd_spike_payments,
    "slow-inventory": cmd_slow_inventory,
    "cascade": cmd_cascade,
    "recover": cmd_recover,
    "load": lambda: asyncio.run(cmd_load()),
}

if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "status"
    if cmd not in COMMANDS:
        print(f"Unknown command: {cmd}")
        print(f"Available: {', '.join(COMMANDS.keys())}")
        sys.exit(1)
    COMMANDS[cmd]()
