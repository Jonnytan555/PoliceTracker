# UK Police Stop & Search – MSSQL + ActiveMQ + FastAPI

**What’s inside**
- SQL Server storage (Bronze JSON → Silver typed → Gold aggregates)
- ActiveMQ Artemis (STOMP) queue
- DownloaderSubject + observers (log, email, ActiveMQ)
- Producer enqueues monthly (force, YYYY-MM) jobs daily
- Worker downloads to bronze and loads to SQL Server
- FastAPI with `/outcomes/last-month` and `/stats/last-month`, plus `/scala` page

## Run locally
```bash
cp .env.sample .env
docker compose up -d db activemq mailhog
docker compose up --build producer worker api

API: http://localhost:8000

GET /outcomes/last-month?force=metropolitan

GET /stats/last-month?force=metropolitan

GET /scala (Scala client example)

MailHog UI (emails): http://localhost:8025

ActiveMQ console: http://localhost:8161
 (admin/admin)

Notes

First start enqueues and processes all available months (backfill). Set START_MONTH in .env to limit.

Respect API limits by keeping MAX_WORKERS modest, or run multiple workers for throughput.