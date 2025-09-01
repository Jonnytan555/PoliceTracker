import json
import logging
import os
from typing import Optional

import stomp    # make sure stomp.py is in requirements

from .email import send_email
from .job_events import JobEvent, Observer # your SMTP helper

class ActiveMQReporter(Observer):
    def __init__(self, host: str, port: int, username: str, password: str, destination: str) -> None:
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.destination = destination

    def update(self, event: JobEvent) -> None:
        payload = {
            "force": event.force,
            "month": event.month,
            "rows": event.rows,
            "inserted": event.inserted,
            "status": event.status,
            "message": event.message,
        }
        logging.info("[ActiveMQReporter] publish -> %s : %s", self.destination, payload)
        conn = stomp.StompConnection12([(self.host, self.port)], keepalive=True)
        try:
            conn.connect(self.username, self.password, wait=True)
            conn.send(self.destination, json.dumps(payload))
        finally:
            try:
                conn.disconnect()
            except Exception:
                pass

class EmailReporter(Observer):
    """
    Uses MailHog in dev by default (SMTP host/port via env).
    Env:
      DL_EMAIL_TO        - required to enable
      DL_EMAIL_FROM      - optional, default "noreply@policetracker.local"
      SMTP_HOST          - default "mailhog"
      SMTP_PORT          - default 1025
    """
    def __init__(self,
                 to: str,
                 sender: Optional[str] = None,
                 host: Optional[str] = None,
                 port: Optional[int] = None) -> None:
        self.to = to
        self.sender = sender or os.getenv("DL_EMAIL_FROM", "noreply@policetracker.local")
        self.host = host or os.getenv("SMTP_HOST", "mailhog")
        self.port = int(port or int(os.getenv("SMTP_PORT", "1025")))

    def update(self, event: JobEvent) -> None:
        subj = f"[PoliceTracker] {event.status.upper()} {event.force} {event.month} ({event.rows} rows, inserted {event.inserted})"
        if event.status != "ok" and event.message:
            subj = f"[PoliceTracker] ERROR {event.force} {event.month}"

        body = f"""
        <h3>Ingestion {event.status}</h3>
        <p>
          <b>Force:</b> {event.force}<br/>
          <b>Month:</b> {event.month}<br/>
          <b>Rows:</b> {event.rows}<br/>
          <b>Inserted:</b> {event.inserted}<br/>
          <b>Status:</b> {event.status}
        </p>
        """
        if event.message:
            body += f"<pre>{event.message}</pre>"

        logging.info("[EmailReporter] sending to %s", self.to)
        send_email(
            sender=self.sender,
            receivers=self.to,
            subject=subj,
            body=body,
            host=self.host,
            port=self.port,
        )

class LogReporter(Observer):
    def update(self, event: JobEvent) -> None:
        logging.info(
            "[LogReporter] %s %s rows=%s inserted=%s status=%s msg=%s",
            event.force, event.month, event.rows, event.inserted, event.status, event.message
        )
