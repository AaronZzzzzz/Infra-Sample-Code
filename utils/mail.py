import smtplib, ssl
from config import email_login
from email.mime.text import MIMEText
from email.header import Header

# Create a secure SSL context

def send_email(subject, message, recipients):

    sender = email_login['account']
    # recipients = ['zhangxiaochen.aaron@gmail.com']

    message = MIMEText(message, 'plain', 'utf-8')
    message['From'] = Header('YYBF Health Monitor', 'utf-8')
    message['To'] = Header('YYBF DevOps', 'utf-8')
    message['Subject'] = Header(subject, 'utf-8')

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", port=465, context=context) as server:
        server.login(sender, email_login['password'])
        server.sendmail(sender, recipients, message.as_string())
