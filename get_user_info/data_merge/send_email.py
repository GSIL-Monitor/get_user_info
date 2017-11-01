import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

import os

class EmailSend(object):
    default_ns_server_id = '/email/smtp_andpay_me'

    @classmethod
    def send_email(cls, subject, to_addrs, body_text=None, attachment_files=None, ns_server_id=None):
        smtp_server_props = dict()
        smtp_server_props['user_name'] = "datasupport@andpay.me"
        smtp_server_props['from_address'] = "datasupport@andpay.me"
        smtp_server_props['server'] = "imap.exmail.qq.com"
        smtp_server_props['password'] = "Data1234"
        smtp_server_props['port'] = "25"
        user_name = smtp_server_props['user_name']

        from_address = smtp_server_props.get('from_address', user_name)
        msg = MIMEMultipart()
        msg['From'] = from_address
        msg['Subject'] = subject
        msg['To'] = ','.join(to_addrs)

        if body_text is not None:
            msg.attach(MIMEText(body_text, 'plain'))

        if attachment_files is not None:
            for attachment_file in attachment_files:
                with open(attachment_file, 'rb') as attachment:
                    part = MIMEBase('application', 'octet-stream')
                    part.set_payload((attachment).read())
                    encoders.encode_base64(part)
                    part.add_header('Content-Disposition', "attachment; filename= %s" % os.path.basename(attachment_file))
                    msg.attach(part)

        ssl_flag = smtp_server_props.get('ssl', False)
        timeout = smtp_server_props.get('timeout', 60)
        if ssl_flag is True:
            server = smtplib.SMTP_SSL(host=smtp_server_props['server'], port=smtp_server_props['port'], timeout=timeout)
        else:
            server = smtplib.SMTP(host=smtp_server_props['server'], port=smtp_server_props['port'], timeout=timeout)

        server.login(user_name, smtp_server_props['password'])
        text = msg.as_string()
        server.sendmail(from_address, to_addrs, text)
        server.quit()