# app/rate_limit.py
import time

class RateLimiter:
    """
    Token-bucket rate limiter.
    rate_per_sec: tokens added per second
    burst: bucket capacity (defaults to ~2x rate or at least 1)
    """
    def __init__(self, rate_per_sec: float, burst: int | None = None):
        assert rate_per_sec > 0
        self.rate = float(rate_per_sec)
        self.capacity = burst if burst is not None else max(1, int(self.rate * 2))
        self.tokens = float(self.capacity)
        self.updated = time.monotonic()

    def acquire(self, tokens: int = 1):
        while True:
            now = time.monotonic()
            delta = now - self.updated
            self.updated = now
            self.tokens = min(self.capacity, self.tokens + delta * self.rate)
            if self.tokens >= tokens:
                self.tokens -= tokens
                return
            sleep = (tokens - self.tokens) / self.rate
            time.sleep(min(sleep, 1.0))
