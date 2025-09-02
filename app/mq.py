# app/mq.py
from __future__ import annotations
import json, logging, os, time
import stomp
from stomp.exception import NotConnectedException

_RETRYABLE = (BrokenPipeError, NotConnectedException, OSError)

class MQClient:
    def __init__(self, host: str, port: int, user: str, password: str):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.heartbeat_ms_out = int(os.getenv("STOMP_HEARTBEAT_OUT_MS", "10000"))
        self.heartbeat_ms_in  = int(os.getenv("STOMP_HEARTBEAT_IN_MS",  "10000"))
        self.conn = stomp.Connection12([(host, port)], keepalive=True)
        self.conn.set_listener("police", _Listener(self))
        self.dlq_on_error = os.getenv("DLQ_ON_ERROR", "1").lower() in ("1","true","yes")
        self.dlq_queue = os.getenv("MQ_QUEUE_DLQ", "/queue/police.dlq")
        self._handler = None

    def connect(self):
        if not self.conn.is_connected():
            self.conn.connect(
                self.user, self.password, wait=True,
                heartbeats=(self.heartbeat_ms_out, self.heartbeat_ms_in),
            )

    def _reconnect(self, delay=0.5):
        try:
            if self.conn.is_connected():
                self.conn.disconnect()
        except Exception:
            pass
        time.sleep(delay)
        self.connect()

    def disconnect(self):
        try:
            if self.conn.is_connected():
                self.conn.disconnect()
        except Exception:
            pass

    def subscribe_json(self, destination: str, handler):
        self._handler = handler
        self.connect()
        self.conn.subscribe(destination=destination, id="police-sub", ack="client-individual")

    def send_json(self, destination: str, obj: dict, _attempt=1):
        try:
            self.connect()
            self.conn.send(destination, json.dumps(obj))
        except _RETRYABLE as e:
            if _attempt <= 2:
                logging.warning("[MQ] send_json retry after %s: %s", type(e).__name__, e)
                self._reconnect()
                return self.send_json(destination, obj, _attempt=_attempt+1)
            raise

    # Helpers used by listener with retry
    def ack(self, message_id: str, subscription: str, _attempt=1):
        try:
            self.conn.ack(message_id, subscription)
        except _RETRYABLE as e:
            if _attempt <= 2:
                logging.warning("[MQ] ack retry after %s: %s", type(e).__name__, e)
                self._reconnect()
                return self.ack(message_id, subscription, _attempt=_attempt+1)
            raise

    def nack(self, message_id: str, subscription: str, _attempt=1):
        try:
            self.conn.nack(message_id, subscription)
        except _RETRYABLE as e:
            if _attempt <= 2:
                logging.warning("[MQ] nack retry after %s: %s", type(e).__name__, e)
                self._reconnect()
                return self.nack(message_id, subscription, _attempt=_attempt+1)
            raise


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
            # Process job
            self.client._handler(body, headers)
            # Ack (with retry)
            self.client.ack(message_id, subscription)

        except Exception as e:
            logging.exception("[MQ] Handler failure")
            if self.client.dlq_on_error and self.client.dlq_queue:
                # Try to publish to DLQ and then ack the original so it doesn't loop
                try:
                    self.client.send_json(self.client.dlq_queue, {
                        "original_body": body,
                        "headers": headers,
                        "error": str(e)
                    })
                    self.client.ack(message_id, subscription)
                except Exception:
                    try:
                        self.client.nack(message_id, subscription)
                    except Exception:
                        pass
            else:
                # No DLQ: request redelivery
                try:
                    self.client.nack(message_id, subscription)
                except Exception:
                    pass

    def on_disconnected(self):
        logging.warning("[MQ] Disconnected, attempting reconnect...")
        try:
            self.client._reconnect()
        except Exception:
            pass
