from __future__ import annotations

import os
import logging
import time

from app.logging_setup import setup_logging
from .config import settings
from .db import get_engine, ensure_schema
from .etl import upsert_bronze_and_silver
from .mq import MQClient

from .job_events import Subject, JobEvent
from .observers import ActiveMQReporter, EmailReporter, LogReporter

logger = setup_logging(
    app="police-tracker",
    filename="logs/police-tracker.log",
    use_stream=True,
    stream_json=True,
    alert_to="test@example.com",
    alert_minimum_level="ERROR",
)

logger.info(
    f"[worker] Using start_month={settings.start_month}, "
    f"forces={settings.forces}, cron='{settings.cron_schedule}'"
)

# ----- Prometheus metrics -----
from .metrics import (
    start_worker_metrics_server,
    JOBS_TOTAL, INGESTED_ROWS_TOTAL,
)

# ----- Rate limiting + backoff HTTP client -----
from .rate_limit import RateLimiter
from .http_client import http_get_with_backoff

# ---- Config ----
MQ_HOST = os.getenv("MQ_HOST", "activemq")
MQ_PORT = int(os.getenv("MQ_PORT", "61613"))
MQ_USER = os.getenv("MQ_USER", "admin")
MQ_PASSWORD = os.getenv("MQ_PASSWORD", "admin")
MQ_QUEUE_FETCH = os.getenv("MQ_QUEUE_FETCH", "/queue/police.fetch")
MQ_QUEUE_DONE  = os.getenv("MQ_QUEUE_DONE",  "/queue/police.done")
MQ_QUEUE_NOTIFY = os.getenv("MQ_QUEUE_NOTIFY", "/queue/police.notify")

# Rate limit config
API_RPS = float(os.getenv("API_RPS", "2"))
API_BURST = int(os.getenv("API_BURST", "4"))
API_MAX_RETRIES = int(os.getenv("API_MAX_RETRIES", "5"))
API_BACKOFF_BASE = float(os.getenv("API_BACKOFF_BASE", "0.5"))
API_BACKOFF_CAP  = float(os.getenv("API_BACKOFF_CAP", "8.0"))

RATE_LIMITER = RateLimiter(API_RPS, burst=API_BURST)

# ---- Observer setup -----
SUBJECT = Subject()
SUBJECT.attach(LogReporter())  # always log
_dl_to = os.getenv("DL_EMAIL_TO", "").strip()
if _dl_to:
    SUBJECT.attach(EmailReporter(to=_dl_to))
if os.getenv("ENABLE_AMQ_REPORTER", "1").lower() in ("1", "true", "yes"):
    SUBJECT.attach(ActiveMQReporter(
        host=MQ_HOST, port=MQ_PORT, username=MQ_USER, password=MQ_PASSWORD, destination=MQ_QUEUE_NOTIFY
    ))

def on_message(body: dict, headers: dict):
    """
    Callback for each job from ActiveMQ.
    body is already a dict: {"force": "...", "month": "YYYY-MM"}
    """
    try:
        force = body.get("force")
        ym    = body.get("month")
        if not force or not ym:
            raise ValueError(f"Bad message: {body}")

        logging.info("[worker] Processing %s %s", force, ym)

        # 1) Fetch raw (rate-limited with backoff)
        RATE_LIMITER.acquire()
        resp = http_get_with_backoff(
            "https://data.police.uk/api/stops-force",
            params={"force": force, "date": ym},
            timeout=60,
            max_retries=API_MAX_RETRIES,
            backoff_base=API_BACKOFF_BASE,
            backoff_cap=API_BACKOFF_CAP,
            force_label=force,
        )
        data = resp.json()
        if not isinstance(data, list):
            data = []
        rows = len(data)

        # 2) Ensure DB schema
        engine = get_engine(settings.database_url)
        ensure_schema(engine)

        # 3) Upsert bronze + silver and refresh gold
        inserted = upsert_bronze_and_silver(engine, force, ym, data)

        # Prometheus: success
        INGESTED_ROWS_TOTAL.labels(force=force).inc(rows)
        JOBS_TOTAL.labels(status="ok").inc()

        # 4) Publish 'done'
        MQClient(MQ_HOST, MQ_PORT, MQ_USER, MQ_PASSWORD).send_json(
            MQ_QUEUE_DONE, {"force": force, "month": ym, "rows": rows, "inserted": inserted, "status": "ok"}
        )

        logging.info("[worker] Completed %s %s: %d rows (inserted %d)", force, ym, rows, inserted)

        # 5) Notify observers (AMQ + Email + Log)
        SUBJECT.notify(JobEvent(force=force, month=ym, rows=rows, inserted=inserted, status="ok"))

    except Exception as e:
        logging.exception("[worker] Error processing job: %s", body)

        # Prometheus: error
        JOBS_TOTAL.labels(status="error").inc()

        # Notify observers about error
        try:
            SUBJECT.notify(JobEvent(
                force=body.get("force","?"), month=body.get("month","?"),
                rows=0, inserted=0, status="error", message=str(e)
            ))
        except Exception:
            pass

        # Emit error message (optional)
        try:
            MQClient(MQ_HOST, MQ_PORT, MQ_USER, MQ_PASSWORD).send_json(
                MQ_QUEUE_DONE,
                {"force": body.get("force"), "month": body.get("month"), "status": "error", "error": str(e)}
            )
        except Exception:
            pass

        # IMPORTANT: return (do not raise) if your mq listener handles DLQ/ack
        return

def main():
    logging.info("[worker] Starting…")
    mq = MQClient(MQ_HOST, MQ_PORT, MQ_USER, MQ_PASSWORD)
    mq.subscribe_json(MQ_QUEUE_FETCH, on_message)
    logging.info("[worker] Subscribed to %s", MQ_QUEUE_FETCH)

    # Start metrics HTTP server
    port = int(os.getenv("METRICS_PORT", "9000"))
    start_worker_metrics_server(port)
    logging.info("[worker] Prometheus metrics on :%s", port)

    while True:
        time.sleep(5)

if __name__ == "__main__":
    main()
