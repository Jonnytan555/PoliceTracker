from pydantic import BaseModel
import os

class Settings(BaseModel):
    database_url: str = os.getenv(
        "DATABASE_URL",
        "mssql+pyodbc://sa:Your_password123@localhost:1433/police?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
    )
    forces: list[str] = os.getenv("FORCES", "metropolitan,west-midlands").split(",")
    cron_schedule: str = os.getenv("CRON_SCHEDULE", "10 3 * * *")
    max_workers: int = int(os.getenv("MAX_WORKERS", "4"))
    start_month: str | None = os.getenv("START_MONTH") or None

settings = Settings()
