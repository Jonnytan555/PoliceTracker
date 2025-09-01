# app/db.py
import re
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

def get_engine(db_url: str) -> Engine:
    return create_engine(db_url, pool_pre_ping=True, future=True)

def _split_batches_on_go(ddl: str) -> list[str]:
    # split on lines that are just "GO" (case-insensitive)
    ddl = ddl.replace('\r\n', '\n').replace('\r', '\n')
    parts = re.split(r'^\s*GO\s*;?\s*$', ddl, flags=re.IGNORECASE | re.MULTILINE)
    return [p.strip() for p in parts if p.strip()]

def ensure_schema(engine: Engine):
    ddl = open('sql/schema.sql', 'r', encoding='utf-8').read()
    batches = _split_batches_on_go(ddl)
    with engine.begin() as conn:
        for i, batch in enumerate(batches, start=1):
            try:
                conn.execute(text(batch))
            except Exception as e:
                preview = batch[:400].replace("\n", "\\n")
                raise RuntimeError(f"DDL batch #{i} failed. Preview: {preview}") from e

def upsert_forces(engine: Engine, forces: list[dict]):
    """
    Upsert the list of forces into dbo.dim_force.
    expects items like {"id": "metropolitan", "name": "Metropolitan Police Service"}
    """
    merge_sql = text("""
MERGE dbo.dim_force AS t
USING (VALUES (:id, :name)) AS s(id, name)
ON (t.id = s.id)
WHEN MATCHED THEN 
    UPDATE SET name = s.name
WHEN NOT MATCHED THEN 
    INSERT (id, name) VALUES (s.id, s.name);
""")
    with engine.begin() as conn:
        for f in forces:
            conn.execute(merge_sql, {"id": f["id"], "name": f["name"]})
