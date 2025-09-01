from __future__ import annotations
from dataclasses import dataclass
from typing import Protocol, List, Optional

@dataclass(frozen=True)
class JobEvent:
    force: str
    month: str
    rows: int = 0
    inserted: int = 0
    status: str = "ok"    # "ok" | "error"
    message: Optional[str] = None

class Observer(Protocol):
    def update(self, event: JobEvent) -> None: ...

class Subject:
    def __init__(self) -> None:
        self._observers: List[Observer] = []

    def attach(self, obs: Observer) -> None:
        if obs not in self._observers:
            self._observers.append(obs)

    def detach(self, obs: Observer) -> None:
        if obs in self._observers:
            self._observers.remove(obs)

    def notify(self, event: JobEvent) -> None:
        # fan out, but never let one observer crash the others
        for obs in list(self._observers):
            try:
                obs.update(event)
            except Exception:
                # swallow to keep pipeline resilient
                import logging
                logging.exception("[observers] observer update failed")
