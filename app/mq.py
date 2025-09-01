# app/mq.py
from __future__ import annotations
import json
import logging
import os
import stomp

class MQClient:
    def __init__(self, host: str, port: int, user: str, password: str):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.conn = stomp.Connection12([(host, port)], keepalive=True)
        self.conn.set_listener("police", _Listener(self))
        self.dlq_on_error = os.getenv("DLQ_ON_ERROR", "1").lower() in ("1","true","yes")
        self.dlq_queue = os.getenv("MQ_QUEUE_DLQ", "/queue/police.dlq")

    def connect(self):
        if not self.conn.is_connected():
            self.conn.connect(self.user, self.password, wait=True)

    def disconnect(self):
        try:
            if self.conn.is_connected():
                self.conn.disconnect()
        except Exception:
            pass

    def subscribe_json(self, destination: str, handler):
        """
        handler(body_dict, headers_dict)
        """
        self.connect()
        # client-individual ack so we can ack/nack per message
        self.conn.subscribe(destination=destination, id="police-sub", ack="client-individual")
        self._handler = handler

    def send_json(self, destination: str, obj: dict):
        self.connect()
        self.conn.send(destination, json.dumps(obj))

class _Listener(stomp.ConnectionListener):
    def __init__(self, client: MQClient):
        self.client = client

    def on_message(self, frame):
        headers = frame.headers
        body_raw = frame.body
        message_id = headers.get("message-id")
        subscription = headers.get("subscription")

        try:
            body = json.loads(body_raw) if body_raw else {}
        except Exception:
            body = {"raw": body_raw}

        try:
            # Process the job
            self.client._handler(body, headers)
            # Ack on success
            self.client.conn.ack(message_id, subscription)

        except Exception as e:
            logging.exception("[MQ] Handler failure")
            # Manual DLQ publish (original payload + error)
            if self.client.dlq_on_error and self.client.dlq_queue:
                try:
                    self.client.send_json(self.client.dlq_queue, {
                        "original_body": body,
                        "headers": headers,
                        "error": str(e)
                    })
                    # Ack original so it does not redeliver forever
                    self.client.conn.ack(message_id, subscription)
                except Exception:
                    # As a last resort, try to nack
                    try:
                        self.client.conn.nack(message_id, subscription)
                    except Exception:
                        pass
            else:
                # Let the broker redeliver
                try:
                    self.client.conn.nack(message_id, subscription)
                except Exception:
                    pass

    def on_disconnected(self):
        logging.warning("[MQ] Disconnected, attempting reconnect...")
        try:
            self.client.connect()
        except Exception:
            pass
