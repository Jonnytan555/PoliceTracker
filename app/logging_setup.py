import json, logging, os, sys, time, socket
from logging.handlers import RotatingFileHandler, SMTPHandler

try:
    from concurrent_log_handler import ConcurrentRotatingFileHandler
    HAS_CONCURRENT = True
except Exception:
    HAS_CONCURRENT = False


# ---------- Formatters ----------
class JsonFormatter(logging.Formatter):
    """JSON formatter that includes common context + exception text."""
    def __init__(self, *, level_as_name=True, extra_static=None):
        super().__init__()
        self.extra_static = extra_static or {}
        self.level_as_name = level_as_name

    def format(self, record: logging.LogRecord) -> str:
        base = {
            "ts": time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(record.created)),
            "level": record.levelname if self.level_as_name else record.levelno,
            "logger": record.name,
            "msg": record.getMessage(),
            "pid": record.process,
            "thread": record.threadName,
            "file": record.filename,
            "line": record.lineno,
        }

        # Pull any logger.extra(...) fields
        reserved = {
            "args","asctime","created","exc_info","exc_text","filename","funcName",
            "levelname","levelno","lineno","module","msecs","message","msg","name",
            "pathname","process","processName","relativeCreated","stack_info","thread",
            "threadName","taskName",
        }
        for k, v in record.__dict__.items():
            if not (k in base or k in reserved or k.startswith("_")):
                base[k] = v

        base.update(self.extra_static)

        if record.exc_info:
            base["exc_info"] = self.formatException(record.exc_info)

        return json.dumps(base, ensure_ascii=False)


class TextFormatter(logging.Formatter):
    """Classic single-line formatter for humans/logfiles/emails."""
    def __init__(self):
        super().__init__("%(asctime)s [%(threadName)-12.12s] [%(levelname)-8.8s] "
                         "[%(filename)s:%(lineno)d] %(message)s")


# ---------- Utilities ----------
def _detect_environment() -> str:
    hostname = socket.gethostname().casefold()
    if hostname.startswith("prd"):
        return "Production"
    if hostname.startswith("tst"):
        return "Test"
    return "Development"


def _ensure_dir(path: str) -> None:
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)


def _parse_level(val: str | int | None, default_env="LOG_LEVEL", default="INFO") -> int:
    if isinstance(val, int):
        return val
    s = (val or os.getenv(default_env, default)).upper()
    return getattr(logging, s, logging.INFO)


def _smtp_handler_mailhog(app: str, environment: str, alert_to: str, level: int) -> SMTPHandler:
    """
    SMTP handler targeting MailHog (or compatible dev SMTP).
    Configure via env if needed:
      SMTP_HOST=localhost  SMTP_PORT=1025  SMTP_FROM=app@local
    """
    smtp_host = os.getenv("SMTP_HOST", "localhost")
    smtp_port = int(os.getenv("SMTP_PORT", "1025"))
    from_addr = os.getenv("SMTP_FROM", f"{app}@local")

    h = SMTPHandler(
        mailhost=(smtp_host, smtp_port),
        fromaddr=from_addr,
        toaddrs=[e.strip() for e in alert_to.split(",") if e.strip()],
        subject=f"[{environment}] Log Alert: {app}",
    )
    h.setLevel(level)
    # Emails are nicer as text
    h.setFormatter(TextFormatter())
    return h


def setup_logging(
    *,
    app: str,
    environment: str | None = None,
    level: str | int | None = None,
    # STDOUT handler
    use_stream: bool = True,
    stream_json: bool = True,
    # File handler
    filename: str | None = None,
    rolling_max_bytes: int = 10 * 1024 * 1024,
    backup_count: int = 10,
    use_concurrent_file_handler: bool = True,
    file_json: bool = False,
    # Alerts (MailHog)
    alert_to: str | None = None,
    alert_minimum_level: str | int = logging.ERROR,
    # Extra context
    extra_static: dict | None = None,
) -> logging.Logger:
    """
    Combine JSON stdout + rotating file + MailHog SMTP alerts.
    Call once at process start.
    """
    env = environment or _detect_environment()
    lvl = _parse_level(level)
    extra_static = {
        "app": app,
        "env": env,
        "host": socket.gethostname(),
        **(extra_static or {}),
    }

    root = logging.getLogger()

    # Clear existing handlers to avoid duplicates on re-init
    for h in list(root.handlers):
        root.removeHandler(h)

    root.setLevel(lvl)

    # Stream handler (stdout)
    if use_stream:
        sh = logging.StreamHandler(sys.stdout)
        sh.setLevel(lvl)
        sh.setFormatter(JsonFormatter(extra_static=extra_static) if stream_json else TextFormatter())
        root.addHandler(sh)

    # File handler (rotating)
    if filename:
        _ensure_dir(filename)
        if use_concurrent_file_handler and HAS_CONCURRENT:
            fh = ConcurrentRotatingFileHandler(filename=filename, maxBytes=rolling_max_bytes, backupCount=backup_count)
        else:
            fh = RotatingFileHandler(filename=filename, maxBytes=rolling_max_bytes, backupCount=backup_count)
        fh.setLevel(lvl)
        fh.setFormatter(JsonFormatter(extra_static=extra_static) if file_json else TextFormatter())
        root.addHandler(fh)

    # MailHog alerts
    if alert_to:
        ah = _smtp_handler_mailhog(app, env, alert_to, _parse_level(alert_minimum_level, default="ERROR"))
        root.addHandler(ah)

    root.propagate = False
    return root


# ---------- Back-compat wrapper (mirrors your earlier signature) ----------
def setup_log(
    app: str,
    environment: str = None,
    minimum_level: int = logging.INFO,
    filename: str = None,
    backup_count: int = 10,
    alert_to: str = None,
    alert_minimum_level: int = logging.ERROR,
    rolling_max_bytes: int = 10 * 1024 * 1024,
    use_concurrent_file_handler=True,
    use_stream=False
):
    """
    Back-compat API: text formatting by default (like your original).
    """
    return setup_logging(
        app=app,
        environment=environment,
        level=minimum_level,
        use_stream=use_stream,
        stream_json=False,
        filename=filename,
        rolling_max_bytes=rolling_max_bytes,
        backup_count=backup_count,
        use_concurrent_file_handler=use_concurrent_file_handler,
        file_json=False,
        alert_to=alert_to,
        alert_minimum_level=alert_minimum_level,
    )


if __name__ == "__main__":
    log = setup_logging(
        app="police-tracker",
        filename="logs/police-tracker.log",
        use_stream=True,
        stream_json=True,
        file_json=False,
        alert_to="test@example.com",
        alert_minimum_level="ERROR",
        level=os.getenv("LOG_LEVEL", "DEBUG"),
    )
    log.info("Service starting", extra={"service_version": "1.2.3"})
    try:
        1 / 0
    except ZeroDivisionError:
        log.exception("Boom")
