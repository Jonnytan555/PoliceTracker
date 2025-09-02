import logging
import os
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from app.logging_setup import setup_logging
from .config import settings
from .db import get_engine, ensure_schema
from .etl import discover_months_for_forces, load_dim_force
from .mq import MQClient


logger = setup_logging(
    app="police-tracker",
    filename="logs/police-tracker.log",
    use_stream=True,
    stream_json=True,
    alert_to="test@example.com",
    alert_minimum_level="ERROR",
)

logger.info(
    f"[producer] Using start_month={settings.start_month}, "
    f"forces={settings.forces}, cron='{settings.cron_schedule}'"
)

MQ_HOST = os.getenv("MQ_HOST", "activemq")
MQ_PORT = int(os.getenv("MQ_PORT", "61613"))
MQ_USER = os.getenv("MQ_USER", "admin")
MQ_PASSWORD = os.getenv("MQ_PASSWORD", "admin")
MQ_QUEUE_FETCH = os.getenv("MQ_QUEUE_FETCH", "/queue/police.fetch")

def enqueue_all():
    engine = get_engine(settings.database_url)
    ensure_schema(engine)

    # 1) ensure forces exist in dim table
    load_dim_force(engine, settings.forces)

    # 2) generate (force, YYYY-MM) pairs from settings.start_month → last full month
    pairs = discover_months_for_forces(settings.start_month, settings.forces)

    # 3) push to MQ
    mq = MQClient(MQ_HOST, MQ_PORT, MQ_USER, MQ_PASSWORD)
    for force_id, ym in pairs:
        mq.send_json(MQ_QUEUE_FETCH, {"force": force_id, "month": ym})
        logging.info(f"[producer] Enqueued {force_id} {ym}")

def main_job():
    enqueue_all()
    logging.info("[producer] Done enqueueing.")

if __name__ == "__main__":
    logging.info(f"[producer] CRON '{settings.cron_schedule}'")
    main_job()
    # schedule the same job based on cron in settings
    sched = BlockingScheduler(timezone="UTC")
    m, h, d, mo, dow = settings.cron_schedule.split()
    sched.add_job(
        main_job,
        CronTrigger(minute=m, hour=h, day=d, month=mo, day_of_week=dow),
        id="daily_producer",
        replace_existing=True,
    )
    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        pass
