"""
Inventory Service — reserves stock.
Also supports chaos injection via env vars.
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

SERVICE_NAME = os.getenv("SERVICE_NAME", "inventory-service")
OTEL_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")

provider = TracerProvider()
exporter = OTLPSpanExporter(endpoint=OTEL_ENDPOINT, insecure=True)
provider.add_span_processor(BatchSpanProcessor(exporter))
trace.set_tracer_provider(provider)
tracer = trace.get_tracer(SERVICE_NAME)

RESERVE_COUNT = Counter("inventory_reserves_total", "Reserve attempts", ["status"])
RESERVE_LATENCY = Histogram("inventory_reserve_duration_seconds", "Reserve latency")

# Fake in-memory inventory, when container restarts, stocks reset to these
STOCK = {"prod-001": 100, "prod-002": 50, "prod-003": 0}

app = FastAPI(title="Inventory Service")
FastAPIInstrumentor.instrument_app(app)


class ReserveRequest(BaseModel):
    product_id: str
    quantity: int


@app.get("/health")
async def health():
    return {"status": "ok", "service": SERVICE_NAME, "stock": STOCK}


@app.get("/debug/config")
async def debug_config():
    return {"failure_rate": int(os.getenv("FAILURE_RATE", "0")), "latency_ms": int(os.getenv("LATENCY_MS", "0"))}


@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/reserve")
async def reserve(req: ReserveRequest):
    start = time.time()

    with tracer.start_as_current_span("reserve_stock") as span:
        span.set_attribute("product.id", req.product_id)
        span.set_attribute("quantity.requested", req.quantity)

        failure_rate = int(os.getenv("FAILURE_RATE", "0"))
        latency_ms = int(os.getenv("LATENCY_MS", "0"))

        if latency_ms > 0:
            time.sleep(latency_ms / 1000)

        if failure_rate > 0 and random.randint(1, 100) <= failure_rate:
            span.set_attribute("error", True)
            RESERVE_COUNT.labels(status="error").inc()
            logger.error(f"[CHAOS] Inventory service crash for {req.product_id}")
            raise HTTPException(status_code=500, detail="Inventory DB connection lost")

        stock = STOCK.get(req.product_id, 0)
        span.set_attribute("quantity.available", stock)

        if stock < req.quantity:
            RESERVE_COUNT.labels(status="out_of_stock").inc()
            logger.warning(f"Out of stock: {req.product_id} (have {stock}, need {req.quantity})")
            raise HTTPException(status_code=409, detail=f"Insufficient stock for {req.product_id}")

        STOCK[req.product_id] -= req.quantity
        RESERVE_COUNT.labels(status="success").inc()
        RESERVE_LATENCY.observe(time.time() - start)
        logger.info(f"Reserved {req.quantity} of {req.product_id}, {STOCK[req.product_id]} remaining")
        return {"status": "reserved", "product_id": req.product_id, "remaining": STOCK[req.product_id]}
