import os
from vyuha.mailer.mailer import *
import sys

def send_mail(subject, to, cc, text, attachment_path = None, extras={}):
    try:
        # Send Mails
        subject = subject
        mail = Email('smtp.gmail.com', 587)
        mail.set_from('alerts@ahwspl.com')
        mail.set_to(to)
        mail.set_cc(cc)
        if attachment_path != None:
            mail.attach(attachment_path)
        mail.set_subject(subject)
        mail.debug = True
        mail.set_mssg_html(text)
        mail.send('p+w8d*zv_2019')
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno, str(e))