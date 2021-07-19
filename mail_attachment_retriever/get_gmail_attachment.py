from imbox import Imbox  # pip install imbox
import traceback
import datetime
import pandas as pd
from pandas.io import gbq
import gmail_credentials
import os
import smtplib


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials.json"
date_time = datetime.datetime.now() + datetime.timedelta(hours=2)

smtp_host = "smtp.gmail.com"
imap_host = "imap.gmail.com"
username = gmail_credentials.username
password = gmail_credentials.password


def send_mail():
    sender_email = username
    sender_password = password
    rec_email = ["any receiver mail address"]
    msg = "Subject: Any subject\r\nAn text for body."

    smtp_obj = smtplib.SMTP(smtp_host, port=587)
    smtp_obj.ehlo()
    smtp_obj.starttls()
    smtp_obj.login(sender_email, sender_password)

    smtp_obj.sendmail(sender_email, rec_email, msg)
    smtp_obj.quit()


def create_dataframe(file):
    df = pd.read_csv(file, delimiter=",")
    df = df.iloc[:, 0:17]
    df.datetime = df.datetime.astype("datetime64")
    df.iloc[:, 1:7] = df.iloc[:, 1:7].astype("object")
    df.iloc[:, 7:18] = df.iloc[:, 7:18].astype("float64")
    return df


def upload_df_to_bq(dataframe):
    project_id = "your-project-id"
    table = "your-table-name"
    bq_schema = [{"name": "datetime", "type": "DATETIME"}]

    dataframe.to_gbq(
        project_id=project_id, destination_table=table, if_exists="replace", table_schema=bq_schema)


def get_mail_attachment():
    attachment_list = []
    mail_folder = "any folder"
    sender_address = "sender address here"
    mail_subject = "any subject"

    mail = Imbox(imap_host, username=username, password=password,
                 ssl=True, ssl_context=None, starttls=False)
    messages = mail.messages(
        folder=mail_folder, unread=True, sent_from=sender_address, subject=mail_subject, date__on=datetime.date.today())  # defaults to inbox

    for (uid, message) in messages:
        for idx, attachment in enumerate(message.attachments):
            try:
                att_fn = attachment.get("filename")
                if ".csv" in att_fn:
                    att_get = attachment.get("content")
                    attachment_list.append(att_get)
                else:
                    print("No CSV file has been found -", date_time)
            except:
                print(traceback.print_exc(), date_time)
        mail.mark_seen(uid)  # optional, mark message as read
    mail.logout()
    return attachment_list


def main():
    mail_attachment = get_mail_attachment()
    df = create_dataframe(mail_attachment[0])
    upload_df_to_bq(df)
    send_mail()
    print("Upload successful -", date_time)


main()
