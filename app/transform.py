# app/transform.py
from __future__ import annotations
from typing import List, Dict
import hashlib

def _hash_record(record: dict) -> str:
    """
    Stable row hash for deduplication.
    Sort keys to ensure deterministic hash.
    """
    raw = str(sorted(record.items()))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

def to_silver_rows(force: str, ym: str, raw: List[Dict]) -> List[Dict]:
    """
    Transform raw Police API stop-and-search JSON (bronze)
    into silver rows ready for SQL upsert.
    """
    silver = []
    for rec in raw:
        row = {
            "force": force,
            "year_month": ym,
            "involved_person": rec.get("involved_person"),
            "datetime": rec.get("datetime"),
            "operation": rec.get("operation"),
            "operation_name": rec.get("operation_name"),
            "location_type": rec.get("location", {}).get("location_type") if rec.get("location") else None,
            "latitude": rec.get("location", {}).get("latitude") if rec.get("location") else None,
            "longitude": rec.get("location", {}).get("longitude") if rec.get("location") else None,
            "gender": rec.get("gender"),
            "age_range": rec.get("age_range"),
            "self_defined_ethnicity": rec.get("self_defined_ethnicity"),
            "officer_defined_ethnicity": rec.get("officer_defined_ethnicity"),
            "legislation": rec.get("legislation"),
            "object_of_search": rec.get("object_of_search"),
            "outcome": rec.get("outcome"),
            "outcome_linked_to_object": rec.get("outcome_linked_to_object_of_search"),
            "removal_of_more_than_outer_clothing": rec.get("removal_of_more_than_outer_clothing"),
            "row_hash": _hash_record(rec),
        }
        silver.append(row)
    return silver
