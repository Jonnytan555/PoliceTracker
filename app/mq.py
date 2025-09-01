# app/mq.py
import json
import logging
import time
import stomp

class _Listener(stomp.ConnectionListener):
    def __init__(self, on_json):
        self.on_json = on_json

    def on_error(self, frame):
        logging.error("[MQ] Broker error: %s", frame.body)

    def on_message(self, frame):
        # With ack='auto', we don't call ack/nack at all.
        try:
            body = json.loads(frame.body) if frame.body else {}
        except Exception:
            body = {}
        headers = frame.headers or {}
        try:
            self.on_json(body, headers)
        except Exception as e:
            # Log and let it fail fast; requeue behavior is broker-dependent with auto-ack
            logging.exception("[MQ] Handler failure: %s", e)

class MQClient:
    def __init__(self, host="activemq", port=61613, user="admin", password="admin", heartbeats=(5000, 5000)):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.heartbeats = heartbeats  # (client, server) ms
        self.conn = None

    def _connect(self):
        conn = stomp.StompConnection12(
            [(self.host, self.port)],
            heartbeats=self.heartbeats
        )
        conn.connect(self.user, self.password, wait=True)
        logging.info("established connection to host %s, port %s", self.host, self.port)
        self.conn = conn

    def subscribe_json(self, destination: str, handler):
        """
        Start a simple, resilient receive loop with auto-ack.
        """
        while True:
            try:
                if self.conn is None or not self.conn.is_connected():
                    self._connect()
                    self.conn.set_listener("", _Listener(handler))
                    # ack='auto' so the client does not call ack/nack
                    self.conn.subscribe(destination=destination, id="sub-1", ack="auto")
                # Block here; stomp.py runs a receiver thread internally.
                time.sleep(5)
            except Exception as e:
                logging.exception("[MQ] connection loop error: %s", e)
                try:
                    if self.conn:
                        self.conn.disconnect()
                except Exception:
                    pass
                self.conn = None
                time.sleep(3)  # backoff

    def send_json(self, destination: str, payload):
        if isinstance(payload, (dict, list)):
            body = json.dumps(payload)
        else:
            body = str(payload)
        if self.conn is None or not self.conn.is_connected():
            self._connect()
        self.conn.send(destination=destination, body=body)
