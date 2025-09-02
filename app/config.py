from __future__ import annotations
from typing import List, Optional
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # ----------------
    # Core / Database
    # ----------------
    database_url: str = Field(..., alias="DATABASE_URL")

    # ----------------
    # Producer / Job selection
    # ----------------
    start_month: str = Field("2022-07", alias="START_MONTH")   # YYYY-MM
    forces_csv: str = Field(
        "metropolitan,west-midlands,city-of-london,avon-and-somerset",
        alias="FORCES"
    )
    cron_schedule: str = Field("10 3 * * *", alias="CRON_SCHEDULE")

    @property
    def forces(self) -> List[str]:
        return _split_csv(self.forces_csv)

    # ----------------
    # MQ / ActiveMQ
    # ----------------
    mq_host: str = Field("activemq", alias="MQ_HOST")
    mq_port: int = Field(61613, alias="MQ_PORT")
    mq_user: str = Field("admin", alias="MQ_USER")
    mq_password: str = Field("admin", alias="MQ_PASSWORD")

    mq_queue_fetch: str = Field("/queue/police.fetch", alias="MQ_QUEUE_FETCH")
    mq_queue_done: str = Field("/queue/police.done", alias="MQ_QUEUE_DONE")
    mq_queue_notify: str = Field("/queue/police.notify", alias="MQ_QUEUE_NOTIFY")
    mq_queue_dlq: str = Field("/queue/police.dlq", alias="MQ_QUEUE_DLQ")
    dlq_on_error: bool = Field(True, alias="DLQ_ON_ERROR")  # 1/0, true/false

    enable_amq_reporter: bool = Field(True, alias="ENABLE_AMQ_REPORTER")

    # ----------------
    # Email / Notifications
    # ----------------
    dl_email_from: str = Field("noreply@policetracker.local", alias="DL_EMAIL_FROM")
    dl_email_to: str = Field("", alias="DL_EMAIL_TO")
    dl_email_cc: str = Field("", alias="DL_EMAIL_CC")
    smtp_host: str = Field("mailhog", alias="SMTP_HOST")
    smtp_port: int = Field(1025, alias="SMTP_PORT")

    # ----------------
    # Metrics
    # ----------------
    metrics_port: int = Field(9000, alias="METRICS_PORT")

    # ----------------
    # Rate limiting / backoff
    # ----------------
    api_rps: float = Field(2.0, alias="API_RPS")
    api_burst: int = Field(4, alias="API_BURST")
    api_max_retries: int = Field(5, alias="API_MAX_RETRIES")
    api_backoff_base: float = Field(0.5, alias="API_BACKOFF_BASE")
    api_backoff_cap: float = Field(8.0, alias="API_BACKOFF_CAP")

    # ----------------
    # Worker / parallelism
    # ----------------
    max_workers: int = Field(4, alias="MAX_WORKERS")

    # ----------------
    # Pydantic settings
    # ----------------
    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=False,
        extra="ignore",
    )

def _split_csv(s: str | None) -> List[str]:
    if not s:
        return []
    return [x.strip() for x in s.split(",") if x.strip()]

settings = Settings()
