from fastapi import FastAPI, Query, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from typing import List
from datetime import date
from sqlalchemy import text
from .db import get_engine
from .utils import last_month_yyyymm
from .config import settings

app = FastAPI(title="Stop & Search API", version="1.0")
templates = Jinja2Templates(directory="app/templates")

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/scala", response_class=HTMLResponse)
def scala_client_page(request: Request):
    return templates.TemplateResponse("scala.html", {"request": request})

@app.get("/outcomes/last-month")
def outcomes_last_month(force: str = Query(..., description="Force ID, e.g., 'metropolitan'")):
    ym = last_month_yyyymm(date.today())
    month_date = f"{ym}-01"
    sql = text("""
        SELECT outcome, [count]
        FROM dbo.gold_monthly_outcomes
        WHERE force_id = :force AND [month] = :month
        ORDER BY [count] DESC, outcome
    """)
    with get_engine(settings.database_url).connect() as conn:
        rows = conn.execute(sql, {"force": force, "month": month_date}).mappings().all()
    return {"force": force, "month": ym, "outcomes": rows}

@app.get("/stats/last-month")
def stats_last_month(force: str = Query(...)):
    ym = last_month_yyyymm(date.today())
    month_date = f"{ym}-01"
    sql = text("""
        SELECT COUNT(*) AS total, 
               SUM(CASE WHEN outcome IS NULL OR outcome = '' THEN 0 ELSE 1 END) AS with_outcome
        FROM dbo.fact_stop_search
        WHERE force_id = :force AND [month] = :month
    """)
    with get_engine(settings.database_url).connect() as conn:
        row = conn.execute(sql, {"force": force, "month": month_date}).mappings().first()
    if not row:
        raise HTTPException(404, "No data")
    return {"force": force, "month": ym, **row}
