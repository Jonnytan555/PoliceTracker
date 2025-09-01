# app/metrics.py
from __future__ import annotations
from prometheus_client import (
    Counter, Histogram, Gauge,
    generate_latest, CONTENT_TYPE_LATEST, REGISTRY, start_http_server
)

# Worker job metrics
JOBS_TOTAL = Counter(
    "police_jobs_total",
    "Jobs processed by the worker",
    ["status"]  # ok|error
)

INGESTED_ROWS_TOTAL = Counter(
    "police_ingested_rows_total",
    "Rows inserted into silver",
    ["force"]
)

# Police API metrics
API_CALLS_TOTAL = Counter(
    "police_api_calls_total",
    "Calls made to the Police API",
    ["force", "outcome"]  # HTTP status code or 'exception'
)

API_LATENCY_SECONDS = Histogram(
    "police_api_latency_seconds",
    "Latency of Police API calls in seconds",
    ["force"]
)

def start_worker_metrics_server(port: int = 9000, addr: str = "0.0.0.0"):
    """
    Starts a tiny HTTP server in the worker that serves / (the metrics payload)
    on the given port. (Prometheus will scrape http://worker:9000/)
    """
    start_http_server(port, addr=addr)

def render_prometheus() -> bytes:
    """
    Use this in FastAPI to render /metrics.
    """
    return generate_latest(REGISTRY)

def content_type() -> str:
    return CONTENT_TYPE_LATEST
