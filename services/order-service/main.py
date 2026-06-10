"""
Order Service — entry point microservice.
Receives orders, calls payment-service and inventory-service.
Fully instrumented with OpenTelemetry traces + Prometheus metrics.
"""

import os
import time
import random
import asyncio
import httpx
import logging

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from starlette.responses import Response

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# OTEL SETUP ie OPEN TELEMETRY
SERVICE_NAME = os.getenv("SERVICE_NAME", "order-service")
OTEL_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")

provider = TracerProvider()
exporter = OTLPSpanExporter(endpoint=OTEL_ENDPOINT, insecure=True)
provider.add_span_processor(BatchSpanProcessor(exporter))
trace.set_tracer_provider(provider)
tracer = trace.get_tracer(SERVICE_NAME)

#PROM PROM SETUP
REQUEST_COUNT = Counter(
    "order_requests_total", "Total order requests", ["method", "endpoint", "status"]
)
REQUEST_LATENCY = Histogram(
    "order_request_duration_seconds", "Order request latency", ["endpoint"]
)
ORDER_SUCCESS = Counter("orders_successful_total", "Successful orders")
ORDER_FAILURE = Counter("orders_failed_total", "Failed orders", ["reason"])

# ── App ────────────────────────────────────────────────────────────────────────
app = FastAPI(title="Order Service")
FastAPIInstrumentor.instrument_app(app)
HTTPXClientInstrumentor().instrument()

PAYMENT_URL = os.getenv("PAYMENT_URL", "http://localhost:8002")
INVENTORY_URL = os.getenv("INVENTORY_URL", "http://localhost:8003")


class OrderRequest(BaseModel):
    order_id: str
    product_id: str
    quantity: int
    amount: float


@app.get("/health")
async def health():
    return {"status": "ok", "service": SERVICE_NAME}


@app.get("/debug/config")
async def debug_config():
    return {"failure_rate": int(os.getenv("FAILURE_RATE", "0")), "latency_ms": int(os.getenv("LATENCY_MS", "0"))}


@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/orders")
async def create_order(order: OrderRequest):
    start = time.time()
    REQUEST_COUNT.labels("POST", "/orders", "started").inc()

    with tracer.start_as_current_span("create_order") as span:
        span.set_attribute("order.id", order.order_id)
        span.set_attribute("order.amount", order.amount)
        span.set_attribute("product.id", order.product_id)

        logger.info(f"Processing order {order.order_id} for product {order.product_id}")

        async with httpx.AsyncClient(timeout=5.0) as client:
            # Step 1: Check inventory
            try:
                with tracer.start_as_current_span("check_inventory"):
                    inv_resp = await client.post(
                        f"{INVENTORY_URL}/reserve",
                        json={"product_id": order.product_id, "quantity": order.quantity},
                    )
                    inv_resp.raise_for_status()
            except Exception as e:
                span.record_exception(e)
                ORDER_FAILURE.labels(reason="inventory_unavailable").inc()
                logger.error(f"Inventory check failed for order {order.order_id}: {e}")
                raise HTTPException(status_code=503, detail=f"Inventory service error: {e}")

            # Step 2: Process payment
            try:
                with tracer.start_as_current_span("process_payment"):
                    pay_resp = await client.post(
                        f"{PAYMENT_URL}/charge",
                        json={"order_id": order.order_id, "amount": order.amount},
                    )
                    pay_resp.raise_for_status()
            except Exception as e:
                span.record_exception(e)
                ORDER_FAILURE.labels(reason="payment_failed").inc()
                logger.error(f"Payment failed for order {order.order_id}: {e}")
                raise HTTPException(status_code=402, detail=f"Payment error: {e}")

        ORDER_SUCCESS.inc()
        REQUEST_LATENCY.labels("/orders").observe(time.time() - start)
        REQUEST_COUNT.labels("POST", "/orders", "200").inc()
        logger.info(f"Order {order.order_id} completed successfully")
        return {"status": "success", "order_id": order.order_id}
