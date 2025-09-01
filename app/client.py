import requests
from tenacity import retry, stop_after_attempt, wait_exponential

BASE = "https://data.police.uk/api"

class RateLimitError(Exception): ...
@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=30), reraise=True)
def _get(url: str, params: dict | None = None):
    r = requests.get(url, params=params, timeout=30)
    if r.status_code == 429:
        raise RateLimitError("Rate limited")
    r.raise_for_status()
    return r.json()

def list_forces():
    return _get(f"{BASE}/forces")

def availability():
    return _get(f"{BASE}/crimes-street-dates")

def stops_by_force(force_id: str, ym: str):
    return _get(f"{BASE}/stops-force", params={"force": force_id, "date": ym})
