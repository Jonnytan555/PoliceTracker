# app/notif/email.py
import smtplib
from email.message import EmailMessage

def send_email(*, sender: str, receivers: str, subject: str, body: str, host: str = "mailhog", port: int = 1025):
    """
    Send a simple HTML email via SMTP (works with MailHog).
    receivers: comma-separated string or single address.
    """
    msg = EmailMessage()
    msg["From"] = sender
    msg["To"] = receivers
    msg["Subject"] = subject
    msg.set_content(body, subtype="html")
    with smtplib.SMTP(host=host, port=port) as s:
        s.send_message(msg)
