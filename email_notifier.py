import smtplib
import logging
from email.message import EmailMessage

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 465

SENDER_EMAIL = "ahmed.khaled.afifi22@gmail.com"
APP_PASSWORD = "xanjopzllovfjiyf"
RECEIVER_EMAIL = "ahmed.khaled.afifi22@gmail.com"


def send_failure_email(error):
    msg = EmailMessage()
    msg["Subject"] = "Spark Job Failed"
    msg["From"] = SENDER_EMAIL
    msg["To"] = RECEIVER_EMAIL

    msg.set_content(
        f"""
Bosta Spark Job FAILED.

Error:
{str(error)}
"""
    )

    try:
        with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as server:
            server.login(SENDER_EMAIL, APP_PASSWORD)
            server.send_message(msg)
    except Exception:
        logging.exception("Failed to send failure email")
