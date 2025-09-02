import os
from fastapi import Depends, FastAPI, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
from starlette.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, generate_latest, Counter

from .logging_setup import setup_logging
from .db import get_engine
from .config import settings


logger = setup_logging(
    app="police-tracker",
    filename="logs/police-tracker.log",
    use_stream=True,
    stream_json=True,
    alert_to="test@example.com",
    alert_minimum_level="ERROR",
)

API_KEY = os.getenv("API_KEY")
ALLOWED_ORIGINS = os.getenv("CORS_ALLOW_ORIGINS", "*").split(",")

app = FastAPI(title="Stop & Search API", version="1.1")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS if ALLOWED_ORIGINS != ["*"] else ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

REGISTRY = CollectorRegistry()
API_REQS = Counter("api_requests_total", "API requests", ["endpoint"], registry=REGISTRY)

@app.middleware("http")
async def metrics_middleware(request, call_next):
    resp = await call_next(request)
    API_REQS.labels(endpoint=request.url.path).inc()
    return resp

def require_api_key(x_api_key: str = Header(default=None)):
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return True

@app.get("/metrics")
def metrics():
    return Response(generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)

@app.get("/")
def root():
    return {"name": "Stop & Search API", "docs": "/docs", "health": "/health"}

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/forces", dependencies=[Depends(require_api_key)])
def list_forces():
    sql = text("SELECT id, name FROM dbo.dim_force ORDER BY name;")
    with get_engine(settings.database_url).connect() as conn:
        rows = conn.execute(sql).mappings().all()
    return {"forces": rows}
