# app/etl_worker.py
import os
import json
import logging
import time
import requests

from .config import settings
from .db import get_engine, ensure_schema
from .etl import upsert_bronze_and_silver
from .mq import MQClient

from .job_events import Subject, JobEvent
from .observers import ActiveMQReporter, EmailReporter, LogReporter

MQ_HOST = os.getenv("MQ_HOST", "activemq")
MQ_PORT = int(os.getenv("MQ_PORT", "61613"))
MQ_USER = os.getenv("MQ_USER", "admin")
MQ_PASSWORD = os.getenv("MQ_PASSWORD", "admin")
MQ_QUEUE_FETCH = os.getenv("MQ_QUEUE_FETCH", "/queue/police.fetch")
MQ_QUEUE_DONE  = os.getenv("MQ_QUEUE_DONE",  "/queue/police.done")
MQ_QUEUE_NOTIFY = os.getenv("MQ_QUEUE_NOTIFY", "/queue/police.notify")

# ---- Observer setup (attach what you need) ----
SUBJECT = Subject()
# Always log
SUBJECT.attach(LogReporter())
# Email only if DL_EMAIL_TO is set
_DL_TO = os.getenv("DL_EMAIL_TO", "").strip()
if _DL_TO:
    SUBJECT.attach(EmailReporter(to=_DL_TO))
# ActiveMQ notifications (optional)
if os.getenv("ENABLE_AMQ_REPORTER", "1") in ("1", "true", "yes"):
    SUBJECT.attach(ActiveMQReporter(
        host=MQ_HOST, port=MQ_PORT, username=MQ_USER, password=MQ_PASSWORD, destination=MQ_QUEUE_NOTIFY
    ))

def on_message(body: dict, headers: dict):
    try:
        force = body.get("force")
        ym    = body.get("month")
        if not force or not ym:
            raise ValueError(f"Bad message: {body}")

        logging.info(f"[worker] Processing {force} {ym}")

        # 1) Fetch raw
        url = "https://data.police.uk/api/stops-force"
        resp = requests.get(url, params={"force": force, "date": ym}, timeout=60)
        resp.raise_for_status()
        data = resp.json() if isinstance(resp.json(), list) else []
        rows = len(data)

        # 2) Schema
        engine = get_engine(settings.database_url)
        ensure_schema(engine)

        # 3) Upsert (bronze + silver + gold)
        inserted = upsert_bronze_and_silver(engine, force, ym, data)

        # 4) Publish 'done' (existing)
        MQClient(MQ_HOST, MQ_PORT, MQ_USER, MQ_PASSWORD).send_json(
            MQ_QUEUE_DONE, {"force": force, "month": ym, "rows": rows, "inserted": inserted, "status": "ok"}
        )

        logging.info(f"[worker] Completed {force} {ym}: {rows} rows (inserted {inserted})")

        # 5) Notify observers (AMQ + Email + Log)
        SUBJECT.notify(JobEvent(force=force, month=ym, rows=rows, inserted=inserted, status="ok"))

    except Exception as e:
        logging.exception(f"[worker] Error processing job: {body}")

        # Notify observers about error
        try:
            SUBJECT.notify(JobEvent(
                force=body.get("force","?"), month=body.get("month","?"),
                rows=0, inserted=0, status="error", message=str(e)
            ))
        except Exception:
            pass

        # Emit error message (existing)
        try:
            MQClient(MQ_HOST, MQ_PORT, MQ_USER, MQ_PASSWORD).send_json(
                MQ_QUEUE_DONE,
                {"force": body.get("force"), "month": body.get("month"), "status": "error", "error": str(e)}
            )
        except Exception:
            pass

        # re-raise so the listener nacks
        raise

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.info("[worker] Starting…")
    mq = MQClient(MQ_HOST, MQ_PORT, MQ_USER, MQ_PASSWORD)
    mq.subscribe_json(MQ_QUEUE_FETCH, on_message)
    logging.info(f"[worker] Subscribed to {MQ_QUEUE_FETCH}")
    while True:
        time.sleep(5)

if __name__ == "__main__":
    main()
