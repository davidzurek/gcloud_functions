import pandas as pd
import urllib.parse
from datetime import datetime, timedelta
from google.cloud import bigquery

table_id = "my_project_id.my_dataset_name.my_table_name"
client = bigquery.Client()


def create_dataframe(timestamp, uri):
    df = pd.DataFrame({
        "timestamp": [timestamp],
        "URI": [uri]
    })
    return df


def create_schema():
    schema = [
        bigquery.SchemaField("timestamp", "TIMESTAMP"),
        bigquery.SchemaField("URI", "STRING")
    ]
    return schema


def create_table(table_id, dataframe, schema):
    table = bigquery.Table(table_id, schema)
    table = client.create_table(table)
    job = client.insert_rows_from_dataframe(
        table=table_id, dataframe=dataframe, selected_fields=schema)


def insert_row(table_id, dataframe, schema):
    job = client.insert_rows_from_dataframe(
        table=table_id, dataframe=dataframe, selected_fields=schema)


def decode_url(request):
    if request.url:
        return urllib.parse.unquote(request.url)


def get_timestamp():
    return datetime.now() + timedelta(hours=2)


def main(request):
    if request.url:
        decoded_url = decode_url(request)
        timestamp = get_timestamp()
        df = create_dataframe(timestamp, decoded_url)
        schema = create_schema()

        try:
            create_table(table_id, df, schema)
        except Exception:
            print(
                f"Table '{table_id}' already exists. New row(s) will be inserted to the existing table instead.")
            insert_row(table_id, df, schema)
        else:
            print(f"Table created: '{table_id}'")
        finally:
            print("Execution completed.")
