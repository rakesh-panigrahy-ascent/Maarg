import imp
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email import encoders
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
import os
import sys

class Email(object):
    def __init__(self, server, port):
        self.server = server
        self.port = port
        self.draft = MIMEMultipart()
        self.recpt = []

    def set_from(self, sender):
        self.sender = sender
        self.draft['From'] = sender

    def set_to(self, recipients):
        self.recpt += recipients
        self.draft['To'] = ','.join(recipients)

    def set_cc(self, recipients):
        self.recpt += recipients
        self.draft['CC'] = ','.join(recipients)

    def set_bcc(self, recipients):
        self.recpt += recipients
        self.draft['BCC'] = ','.join(recipients)

    def set_subject(self, subject):
        self.draft['Subject'] = subject

    def set_mssg(self, mssg):
        self.draft.attach(MIMEText(mssg, 'plain'))

    def set_mssg_html(self, mssg):
        self.draft.attach(MIMEText(mssg, 'html'))
    
    def set_mssg_image(self, image_path, content_id_header):
        content_id_header = '<'+str(content_id_header)+'>'
        image = MIMEImage(open(image_path, 'rb').read())
        image.add_header('Content-ID', content_id_header)
        self.draft.attach(image)

    def attach(self, file):
        try:
            attachment = open(file, "rb")
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', "attachment; filename= %s" % file.split('/')[-1])
            self.draft.attach(part)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno, str(e))

    def send(self, passwd):
        server = smtplib.SMTP(self.server, self.port)
        server.starttls()
        server.login(self.sender, passwd)
        text = self.draft.as_string()
        server.sendmail(self.sender, self.recpt, text)
        server.quit()


def example():
    mail = Email('smtp.gmail.com', 587)
    mail.set_from('notifications@ahwspl.com')
    mail.set_to(['swapnil.gusani@ahwspl.com'])
    mail.set_cc(['nikhil@retailio.in'])
    mail.attach('x4main.py')
    mail.set_subject("Test mail")
    mail.set_mssg("This is a test message.")
    mail.send('notifyme@123')


if __name__ == '__main__':
    example()