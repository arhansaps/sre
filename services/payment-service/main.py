"""
Payment Service — processes charges.
KEY FEATURE: FAILURE_RATE and LATENCY_MS env vars let you
inject failures and latency at runtime to simulate incidents.
"""

import os
import time
import random
import logging

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from starlette.responses import Response

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SERVICE_NAME = os.getenv("SERVICE_NAME", "payment-service")
OTEL_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")

provider = TracerProvider()
exporter = OTLPSpanExporter(endpoint=OTEL_ENDPOINT, insecure=True)
provider.add_span_processor(BatchSpanProcessor(exporter))
trace.set_tracer_provider(provider)
tracer = trace.get_tracer(SERVICE_NAME)

CHARGE_COUNT = Counter("payment_charges_total", "Total charge attempts", ["status"])
CHARGE_LATENCY = Histogram("payment_charge_duration_seconds", "Charge latency")
FAILURE_COUNT = Counter("payment_failures_total", "Payment failures", ["type"])

app = FastAPI(title="Payment Service")
FastAPIInstrumentor.instrument_app(app)


class ChargeRequest(BaseModel):
    order_id: str
    amount: float


def get_failure_rate() -> int:
    try:
        return int(os.getenv("FAILURE_RATE", "0"))
    except ValueError:
        return 0


def get_latency_ms() -> int:
    try:
        return int(os.getenv("LATENCY_MS", "0"))
    except ValueError:
        return 0


@app.get("/health")
async def health():
    return {"status": "ok", "service": SERVICE_NAME}


@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/charge")
async def charge(req: ChargeRequest):
    start = time.time()

    with tracer.start_as_current_span("process_charge") as span:
        span.set_attribute("order.id", req.order_id)
        span.set_attribute("charge.amount", req.amount)

        # ── Inject artificial latency ──────────────────────────────────────────
        latency = get_latency_ms()
        if latency > 0:
            logger.warning(f"[CHAOS] Injecting {latency}ms latency on charge")
            span.set_attribute("chaos.latency_ms", latency)
            time.sleep(latency / 1000)

        # ── Inject failures ────────────────────────────────────────────────────
        failure_rate = get_failure_rate()
        if failure_rate > 0 and random.randint(1, 100) <= failure_rate:
            failure_type = random.choice(["gateway_timeout", "insufficient_funds", "card_declined"])
            FAILURE_COUNT.labels(type=failure_type).inc()
            CHARGE_COUNT.labels(status="failure").inc()
            span.set_attribute("error", True)
            span.set_attribute("chaos.failure_type", failure_type)
            logger.error(f"[CHAOS] Payment failed: {failure_type} for order {req.order_id}")
            raise HTTPException(status_code=500, detail=f"Payment gateway error: {failure_type}")

        # ── Happy path ─────────────────────────────────────────────────────────
        transaction_id = f"txn_{req.order_id}_{int(time.time())}"
        span.set_attribute("transaction.id", transaction_id)
        CHARGE_COUNT.labels(status="success").inc()
        CHARGE_LATENCY.observe(time.time() - start)
        logger.info(f"Charge successful: {transaction_id} for ${req.amount}")
        return {"status": "charged", "transaction_id": transaction_id}
