import hashlib, json
from datetime import datetime, date, timedelta

def sha256_row(obj):
    s = json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(',', ':'))
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def ym_to_date(ym: str) -> str:
    return f"{ym}-01"

def parse_dt(dt_str: str | None):
    if not dt_str:
        return None, None
    try:
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        return dt, dt.date()
    except Exception:
        return None, None

def last_month_yyyymm(today: date | None = None) -> str:
    today = today or date.today()
    first = today.replace(day=1)
    last_month_last_day = first - timedelta(days=1)
    return last_month_last_day.strftime("%Y-%m")
