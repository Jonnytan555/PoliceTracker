# tests/test_transform.py
import datetime as dt
from app.transform import to_silver_rows

def test_to_silver_rows_happy_path():
    # Minimal Police API sample
    payload = [{
        "datetime": "2024-05-01T14:23:00+00:00",
        "involved_person": True,
        "gender": "Male",
        "age_range": "18-24",
        "self_defined_ethnicity": "White",
        "officer_defined_ethnicity": "White",
        "legislation": "PACE s1",
        "object_of_search": "Controlled drugs",
        "outcome": "Arrest",
        "outcome_linked_to_object_of_search": True,
        "outcome_object": {"id": "drug-possession", "name": "Possession of drugs"},
        "removal_more_than_outer_clothing": False,
        "location": {
            "latitude": "51.5074",
            "longitude": "-0.1278",
            "street": {"id": 12345, "name": "Whitehall"}
        },
        "type": "Person search"
    }]
    rows = list(to_silver_rows("metropolitan", "2024-05", payload))
    assert len(rows) == 1
    r = rows[0]
    assert r["force_id"] == "metropolitan"
    assert r["outcome"] == "Arrest"
    assert r["object_of_search"] == "Controlled drugs"
    assert r["street_name"] == "Whitehall"
    assert r["month"] == dt.date(2024, 5, 1)
