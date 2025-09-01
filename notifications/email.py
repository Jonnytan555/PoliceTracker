import smtplib
from email.mime_text import MIMEText
import os

def send_email(senders: str, receivers: str, cc: str, subject: str, body: str):
    host = os.getenv("SMTP_HOST", "localhost")
    port = int(os.getenv("SMTP_PORT", "25"))
    msg = MIMEText(body, "html")
    msg["Subject"] = subject
    msg["From"] = senders
    msg["To"] = receivers
    if cc:
        msg["Cc"] = cc
    targets = [x.strip() for x in receivers.split(",") if x.strip()]
    if cc:
        targets += [x.strip() for x in cc.split(",") if x.strip()]
    with smtplib.SMTP(host=host, port=port) as s:
        s.sendmail(senders, targets, msg.as_string())
