from typing import Iterable
from sqlalchemy import text
from .client import availability, stops_by_force, list_forces
from .db import ensure_schema, get_engine, upsert_forces
from .utils import sha256_row, ym_to_date, parse_dt
from .config import settings
import json

def discover_months_for_forces(forces: list[str]) -> list[tuple[str,str]]:
    months = availability()  # [{ "date": "YYYY-MM", "stop-and-search": [force ids] }, ...]
    pairs = []
    for entry in months:
        ym = entry['date']
        sforces = set(entry.get('stop-and-search', []))
        for f in forces:
            if f in sforces:
                pairs.append((f, ym))
    pairs.reverse()  # chronological
    if settings.start_month:
        pairs = [(f, ym) for (f, ym) in pairs if ym >= settings.start_month]
    return pairs

def load_dim_force(engine):
    forces = list_forces()
    upsert_forces(engine, forces)

def upsert_bronze_and_silver(engine, force_id: str, ym: str, rows: list[dict]) -> int:
    month_date = ym_to_date(ym)
    inserted = 0

    bronze_sql = text("""
INSERT INTO dbo.bronze_stop_search (row_hash, force_id, [month], payload)
SELECT :row_hash, :force_id, :month, :payload
WHERE NOT EXISTS (SELECT 1 FROM dbo.bronze_stop_search WHERE row_hash = :row_hash);
""")

    silver_sql = text("""
INSERT INTO dbo.fact_stop_search (
    row_hash, force_id, stop_datetime, stop_date, [type], involved_person,
    gender, age_range, self_defined_ethnicity, officer_defined_ethnicity, legislation,
    object_of_search, outcome, outcome_linked_to_object_of_search, outcome_object_id,
    outcome_object_name, removal_more_than_outer_clothing, latitude, longitude,
    street_id, street_name, [month]
)
SELECT
    :row_hash, :force_id, :stop_datetime, :stop_date, :type, :involved_person,
    :gender, :age_range, :self_defined_ethnicity, :officer_defined_ethnicity, :legislation,
    :object_of_search, :outcome, :outcome_linked, :outcome_object_id,
    :outcome_object_name, :removal_more_than_outer_clothing, :latitude, :longitude,
    :street_id, :street_name, :month
WHERE NOT EXISTS (SELECT 1 FROM dbo.fact_stop_search WHERE row_hash = :row_hash);
""")

    gold_merge = text("""
;WITH cte AS (
    SELECT force_id, [month], COALESCE(outcome, '') AS outcome, COUNT(*) AS cnt
    FROM dbo.fact_stop_search
    WHERE force_id = :force_id AND [month] = :month
    GROUP BY force_id, [month], COALESCE(outcome, '')
)
MERGE dbo.gold_monthly_outcomes AS t
USING cte s
ON (t.force_id = s.force_id AND t.[month] = s.[month] AND ISNULL(t.outcome,'') = s.outcome)
WHEN MATCHED THEN UPDATE SET [count] = s.cnt
WHEN NOT MATCHED THEN INSERT (force_id, [month], outcome, [count]) VALUES (s.force_id, s.[month], s.outcome, s.cnt);
""")

    with engine.begin() as conn:
        for r in rows:
            row_hash = sha256_row({"force": force_id, "month": ym, "payload": r})
            payload = json.dumps(r, ensure_ascii=False, separators=(',', ':'))
            conn.execute(bronze_sql, {"row_hash": row_hash, "force_id": force_id, "month": month_date, "payload": payload})

            dt, d = parse_dt(r.get("datetime"))
            loc = r.get("location") or {}
            street = (loc or {}).get("street") or {}

            conn.execute(silver_sql, {
                "row_hash": row_hash,
                "force_id": force_id,
                "stop_datetime": dt,
                "stop_date": d,
                "type": r.get("type"),
                "involved_person": r.get("involved_person"),
                "gender": r.get("gender"),
                "age_range": r.get("age_range"),
                "self_defined_ethnicity": r.get("self_defined_ethnicity"),
                "officer_defined_ethnicity": r.get("officer_defined_ethnicity"),
                "legislation": r.get("legislation"),
                "object_of_search": r.get("object_of_search"),
                "outcome": r.get("outcome") if r.get("outcome") not in (False, None) else "",
                "outcome_linked": r.get("outcome_linked_to_object_of_search"),
                "outcome_object_id": (r.get("outcome_object") or {}).get("id"),
                "outcome_object_name": (r.get("outcome_object") or {}).get("name"),
                "removal_more_than_outer_clothing": r.get("removal_of_more_than_outer_clothing"),
                "latitude": float(loc.get("latitude")) if loc and loc.get("latitude") else None,
                "longitude": float(loc.get("longitude")) if loc and loc.get("longitude") else None,
                "street_id": street.get("id"),
                "street_name": street.get("name"),
                "month": month_date,
            })
            inserted += 1

        conn.execute(gold_merge, {"force_id": force_id, "month": month_date})

    return inserted
