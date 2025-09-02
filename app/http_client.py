from __future__ import annotations
import time, random
import requests

from .metrics import API_LATENCY_SECONDS, API_CALLS_TOTAL

_RETRYABLE_STATUSES = {429, 500, 502, 503, 504}

def http_get_with_backoff(
    url: str,
    *,
    params: dict | None = None,
    timeout: int = 60,
    max_retries: int = 5,
    backoff_base: float = 0.5,
    backoff_cap: float = 10.0,
    force_label: str | None = None,
):
    """
    GET with exponential backoff + jitter on network errors and 429/5xx.
    Emits Prometheus metrics for latency and call outcomes.
    """
    attempt = 0
    while True:
        start = time.time()
        try:
            resp = requests.get(url, params=params, timeout=timeout)
            duration = time.time() - start
            if force_label:
                API_LATENCY_SECONDS.labels(force=force_label).observe(duration)
                API_CALLS_TOTAL.labels(force=force_label, outcome=str(resp.status_code)).inc()

            if resp.status_code in _RETRYABLE_STATUSES:
                # treat as retryable error
                if attempt >= max_retries:
                    resp.raise_for_status()
                _sleep_with_jitter(attempt, backoff_base, backoff_cap)
                attempt += 1
                continue

            resp.raise_for_status()
            return resp

        except Exception:
            duration = time.time() - start
            if force_label:
                API_LATENCY_SECONDS.labels(force=force_label).observe(duration)
                API_CALLS_TOTAL.labels(force=force_label, outcome="exception").inc()
            if attempt >= max_retries:
                raise
            _sleep_with_jitter(attempt, backoff_base, backoff_cap)
            attempt += 1

def _sleep_with_jitter(attempt: int, base: float, cap: float):
    # Exponential backoff (base * 2^attempt) with full jitter, capped
    delay = min(cap, base * (2 ** attempt))
    delay *= random.uniform(0.5, 1.5)  # jitter
    time.sleep(delay)
