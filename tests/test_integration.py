# tests/test_integration_roundtrip.py
import os
import pytest
from sqlalchemy import create_engine, text
from app.db import ensure_schema
from app.etl import upsert_bronze_and_silver

DATABASE_URL = os.getenv("DATABASE_URL_TEST", os.getenv("DATABASE_URL"))

@pytest.mark.integration
def test_roundtrip_one_month():
    engine = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)
    ensure_schema(engine)

    force = "metropolitan"
    ym = "2024-05"
    # small fake dataset
    data = [
        {"datetime": "2024-05-10T12:00:00+00:00", "type": "Person search", "outcome": "Nothing found",
         "location": {"latitude": "51.5", "longitude": "-0.1", "street": {"id": 1, "name": "Some St"}}},
        {"datetime": "2024-05-10T13:00:00+00:00", "type": "Person search", "outcome": "Arrest",
         "location": {"latitude": "51.5", "longitude": "-0.1", "street": {"id": 2, "name": "Other St"}}},
    ]

    inserted = upsert_bronze_and_silver(engine, force, ym, data)
    assert inserted == 2

    with engine.connect() as conn:
        silver_count = conn.execute(
            text("SELECT COUNT(*) FROM dbo.fact_stop_search WHERE force_id=:f AND [month]=:m"),
            {"f": force, "m": f"{ym}-01"},
        ).scalar_one()
        assert silver_count >= 2

        gold = conn.execute(
            text("""SELECT outcome, [count] FROM dbo.gold_monthly_outcomes
                    WHERE force_id=:f AND [month]=:m"""),
            {"f": force, "m": f"{ym}-01"},
        ).mappings().all()
        assert any(row["outcome"] == "Arrest" for row in gold)
