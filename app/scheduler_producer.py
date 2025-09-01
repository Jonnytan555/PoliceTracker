import os
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from .config import settings
from .db import get_engine, ensure_schema
from .etl import discover_months_for_forces, load_dim_force
from .mq import MQClient

MQ_HOST = os.getenv("MQ_HOST", "activemq")
MQ_PORT = int(os.getenv("MQ_PORT", "61613"))
MQ_USER = os.getenv("MQ_USER", "admin")
MQ_PASSWORD = os.getenv("MQ_PASSWORD", "admin")
MQ_QUEUE_FETCH = os.getenv("MQ_QUEUE_FETCH", "/queue/police.fetch")

def enqueue_all():
    engine = get_engine(settings.database_url)
    ensure_schema(engine)
    load_dim_force(engine)
    pairs = discover_months_for_forces(settings.forces)
    mq = MQClient(MQ_HOST, MQ_PORT, MQ_USER, MQ_PASSWORD)
    for force_id, ym in pairs:
        mq.send_json(MQ_QUEUE_FETCH, {"force": force_id, "month": ym})
        print(f"[producer] Enqueued {force_id} {ym}")

def main_job():
    enqueue_all()
    print("[producer] Done enqueueing.")

if __name__ == "__main__":
    print(f"[producer] CRON '{settings.cron_schedule}'")
    main_job()
    sched = BlockingScheduler(timezone="UTC")
    m, h, d, mo, dow = settings.cron_schedule.split()
    sched.add_job(main_job, CronTrigger(minute=m, hour=h, day=d, month=mo, day_of_week=dow),
                  id="daily_producer", replace_existing=True)
    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        pass
