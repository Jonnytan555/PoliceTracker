# app/etl.py
from __future__ import annotations

import datetime as dt
import json
from typing import Iterable, List, Dict, Tuple

from sqlalchemy import text
from sqlalchemy.engine import Engine

from .transform import to_silver_rows


# -----------------------
# Helpers
# -----------------------

def _month_first_day(ym: str) -> dt.date:
    """'YYYY-MM' -> date(YYYY, MM, 1)"""
    y, m = ym.split("-")
    return dt.date(int(y), int(m), 1)


def _force_display_name(force_id: str) -> str:
    # crude pretty-name; replace with authoritative lookup if you have one
    return force_id.replace("-", " ").title()


# -----------------------
# Bronze
# -----------------------

def upsert_bronze(engine: Engine, force: str, ym: str, raw_records: List[Dict]) -> int:
    """
    Insert raw JSON rows into dbo.bronze_stop_search.
    One row per source record (idempotency at bronze is not required; keep raw history).
    """
    if not raw_records:
        return 0

    rows = [
        {
            "force_id": force,
            "month": _month_first_day(ym),
            "raw_json": json.dumps(rec, ensure_ascii=False),
        }
        for rec in raw_records
    ]

    sql = text("""
        INSERT INTO dbo.bronze_stop_search (force_id, [month], raw_json)
        VALUES (:force_id, :month, :raw_json)
    """)
    # Use fast_executemany when pyodbc is present (SQLAlchemy 2.x sets it automatically on connection)
    with engine.begin() as conn:
        conn.execute(sql, rows)
    return len(rows)


# -----------------------
# Silver
# -----------------------

def upsert_silver(engine: Engine, force: str, ym: str, raw_records: List[Dict]) -> int:
    """
    Transform raw JSON -> silver rows; upsert into dbo.fact_stop_search by row_hash.
    Returns number of rows inserted (new).
    """
    month_date = _month_first_day(ym)
    silver_rows = list(to_silver_rows(force, ym, raw_records))
    if not silver_rows:
        return 0

    # Load rows into a temp table for fast, set-based MERGE
    with engine.begin() as conn:
        # Create temp table
        conn.execute(text("""
            IF OBJECT_ID('tempdb..#silver_in') IS NOT NULL DROP TABLE #silver_in;
            CREATE TABLE #silver_in (
                row_hash CHAR(64) NOT NULL,
                force_id NVARCHAR(100) NOT NULL,
                stop_datetime DATETIME2(0) NULL,
                [type] NVARCHAR(200) NULL,
                involved_person BIT NULL,
                gender NVARCHAR(50) NULL,
                age_range NVARCHAR(50) NULL,
                self_defined_ethnicity NVARCHAR(200) NULL,
                officer_defined_ethnicity NVARCHAR(200) NULL,
                legislation NVARCHAR(400) NULL,
                object_of_search NVARCHAR(400) NULL,
                outcome NVARCHAR(200) NULL,
                outcome_linked_to_object_of_search BIT NULL,
                outcome_object_id NVARCHAR(100) NULL,
                outcome_object_name NVARCHAR(200) NULL,
                removal_more_than_outer_clothing BIT NULL,
                latitude FLOAT NULL,
                longitude FLOAT NULL,
                street_id BIGINT NULL,
                street_name NVARCHAR(300) NULL,
                [month] DATE NOT NULL
            );
        """))

        # Prepare rows with all columns expected by #silver_in
        payload = []
        for r in silver_rows:
            payload.append({
                "row_hash": r["row_hash"],
                "force_id": r.get("force_id") or r.get("force") or force,
                "stop_datetime": r.get("stop_datetime") or r.get("datetime"),
                "type": r.get("type"),
                "involved_person": r.get("involved_person"),
                "gender": r.get("gender"),
                "age_range": r.get("age_range"),
                "self_defined_ethnicity": r.get("self_defined_ethnicity"),
                "officer_defined_ethnicity": r.get("officer_defined_ethnicity"),
                "legislation": r.get("legislation"),
                "object_of_search": r.get("object_of_search"),
                "outcome": r.get("outcome") or "",
                "outcome_linked_to_object_of_search": r.get("outcome_linked_to_object_of_search")
                                                       or r.get("outcome_linked_to_object"),
                "outcome_object_id": r.get("outcome_object_id"),
                "outcome_object_name": r.get("outcome_object_name"),
                "removal_more_than_outer_clothing": r.get("removal_more_than_outer_clothing")
                                                      or r.get("removal_of_more_than_outer_clothing"),
                "latitude": r.get("latitude"),
                "longitude": r.get("longitude"),
                "street_id": r.get("street_id"),
                "street_name": r.get("street_name"),
                "month": r.get("month") or month_date,
            })

        # Bulk insert into temp table
        conn.execute(text("""
            INSERT INTO #silver_in (
                row_hash, force_id, stop_datetime, [type], involved_person, gender, age_range,
                self_defined_ethnicity, officer_defined_ethnicity, legislation, object_of_search, outcome,
                outcome_linked_to_object_of_search, outcome_object_id, outcome_object_name,
                removal_more_than_outer_clothing, latitude, longitude, street_id, street_name, [month]
            )
            VALUES (
                :row_hash, :force_id, :stop_datetime, :type, :involved_person, :gender, :age_range,
                :self_defined_ethnicity, :officer_defined_ethnicity, :legislation, :object_of_search, :outcome,
                :outcome_linked_to_object_of_search, :outcome_object_id, :outcome_object_name,
                :removal_more_than_outer_clothing, :latitude, :longitude, :street_id, :street_name, :month
            )
        """), payload)

        # MERGE into silver fact
        result = conn.execute(text("""
            MERGE dbo.fact_stop_search AS tgt
            USING #silver_in AS s
            ON (tgt.row_hash = s.row_hash)
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (
                    row_hash, force_id, stop_datetime, [type], involved_person, gender, age_range,
                    self_defined_ethnicity, officer_defined_ethnicity, legislation, object_of_search, outcome,
                    outcome_linked_to_object_of_search, outcome_object_id, outcome_object_name,
                    removal_more_than_outer_clothing, latitude, longitude, street_id, street_name, [month]
                )
                VALUES (
                    s.row_hash, s.force_id, s.stop_datetime, s.[type], s.involved_person, s.gender, s.age_range,
                    s.self_defined_ethnicity, s.officer_defined_ethnicity, s.legislation, s.object_of_search, s.outcome,
                    s.outcome_linked_to_object_of_search, s.outcome_object_id, s.outcome_object_name,
                    s.removal_more_than_outer_clothing, s.latitude, s.longitude, s.street_id, s.street_name, s.[month]
                )
            WHEN MATCHED THEN
                UPDATE SET
                    tgt.outcome = s.outcome,
                    tgt.street_name = s.street_name,
                    tgt.latitude = s.latitude,
                    tgt.longitude = s.longitude,
                    tgt.officer_defined_ethnicity = s.officer_defined_ethnicity,
                    tgt.self_defined_ethnicity = s.self_defined_ethnicity
            OUTPUT $action AS merge_action;
        """))

        # Count inserts from MERGE output
        inserted = sum(1 for row in result if row.merge_action == "INSERT")
        return inserted


# -----------------------
# Gold
# -----------------------

def refresh_gold_month(engine: Engine, force: str, ym: str) -> int:
    """
    Rebuild gold aggregation (monthly outcomes) for the given force & month.
    Returns number of gold rows affected.
    """
    month_date = _month_first_day(ym)
    with engine.begin() as conn:
        # Aggregate from silver for the month/force
        conn.execute(text("""
            IF OBJECT_ID('tempdb..#agg') IS NOT NULL DROP TABLE #agg;
            SELECT
                force_id,
                [month],
                NULLIF(LTRIM(RTRIM(outcome)), N'') AS outcome,
                COUNT(*) AS cnt
            INTO #agg
            FROM dbo.fact_stop_search WITH (NOLOCK)
            WHERE force_id = :force AND [month] = :month
            GROUP BY force_id, [month], NULLIF(LTRIM(RTRIM(outcome)), N'');
        """), {"force": force, "month": month_date})

        # Upsert into gold
        result = conn.execute(text("""
            MERGE dbo.gold_monthly_outcomes AS tgt
            USING (
                SELECT force_id, [month],
                       COALESCE(outcome, N'Unknown') AS outcome,
                       cnt
                FROM #agg
            ) AS a
            ON (tgt.force_id = a.force_id AND tgt.[month] = a.[month] AND tgt.outcome = a.outcome)
            WHEN NOT MATCHED THEN
                INSERT (force_id, [month], outcome, [count])
                VALUES (a.force_id, a.[month], a.outcome, a.cnt)
            WHEN MATCHED THEN
                UPDATE SET tgt.[count] = a.cnt
            OUTPUT $action AS merge_action;
        """))

        changed = sum(1 for row in result)  # INSERT or UPDATE rows counted
        return changed


# -----------------------
# Orchestration called by worker
# -----------------------

def upsert_bronze_and_silver(engine: Engine, force: str, ym: str, raw_records: List[Dict]) -> int:
    """
    Entry-point used by the worker:
      - write bronze
      - upsert silver
      - refresh gold for that slice
    Returns inserted count for silver.
    """
    # Bronze (raw history)
    upsert_bronze(engine, force, ym, raw_records)
    # Silver
    inserted = upsert_silver(engine, force, ym, raw_records)
    # Gold
    refresh_gold_month(engine, force, ym)
    return inserted


# -----------------------
# Utilities for producer
# -----------------------

def discover_months_for_forces(start_month: str, forces: Iterable[str]) -> List[Tuple[str, str]]:
    """
    From start_month (YYYY-MM) to last full month (inclusive), produce (force, ym) pairs.
    """
    y, m = map(int, start_month.split("-"))
    start = dt.date(y, m, 1)
    today = dt.date.today().replace(day=1)
    last_full = (today - dt.timedelta(days=1)).replace(day=1)  # previous month first day

    jobs: List[Tuple[str, str]] = []
    cur = start
    while cur <= last_full:
        ym = f"{cur.year:04d}-{cur.month:02d}"
        for force in forces:
            jobs.append((force, ym))
        # next month
        if cur.month == 12:
            cur = dt.date(cur.year + 1, 1, 1)
        else:
            cur = dt.date(cur.year, cur.month + 1, 1)
    return jobs


def load_dim_force(engine: Engine, forces: Iterable[str]) -> int:
    """
    Ensure dbo.dim_force has the list of forces (id, name).
    Returns number of upserts.
    """
    rows = [{"id": f, "name": _force_display_name(f)} for f in forces]
    if not rows:
        return 0
    with engine.begin() as conn:
        # temp table
        conn.execute(text("""
            IF OBJECT_ID('tempdb..#forces') IS NOT NULL DROP TABLE #forces;
            CREATE TABLE #forces (id NVARCHAR(100) NOT NULL, name NVARCHAR(255) NOT NULL);
        """))
        conn.execute(text("INSERT INTO #forces (id, name) VALUES (:id, :name)"), rows)
        result = conn.execute(text("""
            MERGE dbo.dim_force AS tgt
            USING #forces AS s
            ON (tgt.id = s.id)
            WHEN NOT MATCHED THEN
                INSERT (id, name) VALUES (s.id, s.name)
            WHEN MATCHED AND ISNULL(tgt.name, N'') <> ISNULL(s.name, N'') THEN
                UPDATE SET tgt.name = s.name
            OUTPUT $action AS merge_action;
        """))
        return sum(1 for _ in result)
