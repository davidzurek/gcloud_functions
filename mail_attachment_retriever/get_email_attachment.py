import email
import imaplib
import smtplib
import datetime as dt
import pandas as pd
import os
import io
import gmail_credentials


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials2.json"

smtp_host = "smtp.gmail.com"
imap_host = "imap.gmail.com"
username = gmail_credentials.username
password = gmail_credentials.password


def get_date():
    today = dt.date.today()
    today_minus_seven = dt.date.today() - dt.timedelta(days=7)
    today_rearranged = dt.datetime.strptime(
        str(today), "%Y-%m-%d").strftime("%d-%b-%Y")
    today_rearranged_minus_seven = dt.datetime.strptime(
        str(today_minus_seven), "%Y-%m-%d").strftime("%d-%b-%Y")
    return today_rearranged, today_rearranged_minus_seven


def send_mail(datetime):
    sender_email = username
    sender_password = password
    # rec_email = ["d.zurek@pa.ag", "martech-analytics@pa.ag", "martech@pa.ag"]
    rec_email = ["d.zurek@pa.ag"]
    msg = "Subject: SumUp report has been updated at {}\r\nData upload to BQ successfully completed. \n\nKind regards, \nTeam MarTech".format(
        datetime[0])

    smtp_obj = smtplib.SMTP(smtp_host, port=587)
    smtp_obj.ehlo()
    smtp_obj.starttls()
    smtp_obj.login(sender_email, sender_password)

    smtp_obj.sendmail(sender_email, rec_email, msg)
    smtp_obj.quit()


def create_dataframe(file):
    df = pd.read_csv(file, delimiter=";")
    df = df.iloc[:, 0:17]
    df.datetime = df.datetime.astype("datetime64")
    df.iloc[:, 1:7] = df.iloc[:, 1:7].astype("object")
    df.iloc[:, 7:18] = df.iloc[:, 7:18].astype("float64")
    return df


def upload_df_to_bq(dataframe):
    project_id = "pa-internal-projects"
    table = "sumup_reporting_data.csv_test_2"
    bq_schema = [{"name": "datetime", "type": "DATETIME"}]

    dataframe.to_gbq(
        project_id=project_id, destination_table=table, if_exists="replace", table_schema=bq_schema)


def search_mail():
    mail_folder = "sumup_report"
    sender_address = "om-data@sumup.com"
    mail_subject = "performance report"

    return mail_folder, sender_address, mail_subject


def delete_mail(mail_search, datetime):
    last_seven_days = datetime[1]

    mail = imaplib.IMAP4_SSL(imap_host)
    mail.login(username, password)
    mail.select(mail_search[0])

    _, search_data = mail.search(
        None, f'(FROM {mail_search[1]})', f'(SUBJECT "{mail_search[2]}")', f'(BEFORE {last_seven_days})')

    for num in search_data[0].split():
        mail.store(num, "+FLAGS", "\\Deleted")
        mail.expunge()


def get_mail_attachment(mail_search, datetime):

    att_list = []

    today = datetime[0]

    mail = imaplib.IMAP4_SSL(imap_host)
    mail.login(username, password)
    mail.select(mail_search[0])

    _, search_data = mail.search(
        None, f'(SUBJECT "{mail_search[2]}")', f"FROM {mail_search[1]}", "UNSEEN", f"ON {today}")
    for num in search_data[0].split():
        _, data = mail.fetch(num, "(RFC822)")
        _, b = data[0]
        email_message = email.message_from_bytes(b)

        for part in email_message.walk():
            if part.get_content_type() == "application/octet-stream" or ".csv" in str(part.get_filename()):
                body = part.get_payload(decode=True)
                file = io.BytesIO(body)
                att_list.append(file)

    mail.logout()
    return att_list


def main():
    date = get_date()
    mail_search = search_mail()
    mail_attachment = get_mail_attachment(mail_search, date)
    delete_mail(mail_search, date)
    if len(mail_attachment) == 0:
        print("No attachment has been found.")
    else:
        df = create_dataframe(mail_attachment[0])
        upload_df_to_bq(df)
        send_mail(date)
        print("Upload successful -", date[0])


main()
