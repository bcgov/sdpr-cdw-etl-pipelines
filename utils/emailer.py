import smtplib
from email.message import EmailMessage
from email.headerregistry import Address

class Emailer:
    
    def __init__(self):
        self.host = "apps.smtp.gov.bc.ca"

    def email(self, subject, from_name, from_email, to_name, to_email, message):
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = Address(display_name=from_name, addr_spec=from_email)
        msg["To"] = Address(display_name=to_name, addr_spec=to_email)
        msg.set_content(message)

        with smtplib.SMTP(self.host) as s:
            s.send_message(msg)
