# app/etl_worker.py
import json
import logging
import time
import requests

from .config import settings
from .db import get_engine, ensure_schema
from .etl import upsert_bronze_and_silver
from .mq import MQClient

MQ_HOST = "activemq"
MQ_PORT = 61613
MQ_USER = "admin"
MQ_PASSWORD = "admin"
MQ_QUEUE_FETCH = "/queue/police.fetch"
MQ_QUEUE_DONE  = "/queue/police.done"

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

        logging.info(f"[worker] Processing {force} {ym}")

        # 1) Fetch raw data from Police API
        url = "https://data.police.uk/api/stops-force"
        resp = requests.get(url, params={"force": force, "date": ym}, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, list):
            data = []

        # 2) Ensure DB schema
        engine = get_engine(settings.database_url)
        ensure_schema(engine)

        # 3) Upsert bronze + silver and refresh gold
        inserted = upsert_bronze_and_silver(engine, force, ym, data)

        # 4) Publish done
        MQClient(MQ_HOST, MQ_PORT, MQ_USER, MQ_PASSWORD).send_json(
            MQ_QUEUE_DONE,
            {"force": force, "month": ym, "rows": len(data), "inserted": inserted, "status": "ok"}
        )
        logging.info(f"[worker] Completed {force} {ym}: {len(data)} rows (inserted {inserted})")

    except Exception as e:
        logging.exception(f"[worker] Error processing job: {body}")
        # emit error message (optional)
        try:
            MQClient(MQ_HOST, MQ_PORT, MQ_USER, MQ_PASSWORD).send_json(
                MQ_QUEUE_DONE,
                {"force": body.get("force"), "month": body.get("month"), "status": "error", "error": str(e)}
            )
        except Exception:
            pass
        # re-raise to NACK via listener
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
