import os
import smtplib
import requests
from importlib import import_module
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from daemonizer.conf import settings

class Mailer:
    """
    Mailer App for mails distribution
    Currently we support mailgun and gmail with proper cerdentials provided
    """

    def __init__(self, mailer=None):
        """Initialization"""
        if not mailer:
            self.mailer_config = settings.DEFAULT_MAILER_CONFIG
        else:
            self.mailer_config = settings.MAILERS.get(mailer)
        
        if not self.mailer_config:
            raise Exception("Mailer '%s' not found!" % (mailer, ))

        self.mailer_type = self.mailer_config.get('type')

    def send_mail(self, subject, message, recipients, is_html=False):
        if self.mailer_type == 'MAILGUN':
            self.send_mail_from_mailgun(subject, message, recipients, is_html)
        elif self.mailer_type == 'GMAIL':
            self.send_mail_from_gmail(subject, message, recipients, is_html)

    def send_mail_from_mailgun(self, subject, message, recipients, is_html=False):
        request_data = {
                        'from': self.mailer_config.get('sender'),
                        'to': recipients,
                        'subject': subject
                    }

        if is_html:
            request_data['html'] = message
        else:
            request_data['text'] = message

        request = requests.post(
                                self.mailer_config.get('url'),
                                auth=('api', self.mailer_config.get('key')),
                                data=request_data
                            )
        return request.status_code

    def send_mail_from_gmail(self, subject, message, recipients, is_html=False):
        server = smtplib.SMTP('smtp.gmail.com:587')
        sender = self.mailer_config.get('sender')
        password = self.mailer_config.get('password')

        mail_data = MIMEMultipart('alternative')
        mail_data['subject'] = subject
        if type(recipients) in [tuple, list]:
            mail_data['To'] = ','.join(recipients)
        else:
            mail_data['To'] = recipients
        mail_data['From'] = sender
      
        # Record the MIME type text/html.
        HTML_BODY = MIMEText(message, 'html')
     
        # Attach parts into message container.
        # According to RFC 2046, the last part of a multipart message, in this case
        # the HTML message, is best and preferred.
        mail_data.attach(HTML_BODY)
     
        server.starttls()
        server.login(sender, password)
        server.sendmail(sender, recipients, mail_data.as_string())
        server.quit()