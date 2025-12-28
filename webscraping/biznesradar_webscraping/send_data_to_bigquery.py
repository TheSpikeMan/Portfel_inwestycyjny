from google.cloud import bigquery
from dotenv import load_dotenv
from pathlib import Path
import os
import pandas as pd

""" Pobieram zmienne środowiskowe """
load_dotenv()
project_id = os.getenv("BQ_PROJECT_ID")
dataset_id = os.getenv("BQ_DATASET_RAW")
table_id = os.getenv("BQ_TABLE_RAW_DATA")


def send_data_to_bigquery(df: pd.DataFrame):

    """ Inicjuję klienta BigQuery """
    client = bigquery.Client()
    table = f"{project_id}.{dataset_id}.{table_id}"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        create_disposition="CREATE_IF_NEEDED",
        clustering_fields=['Timestamp', 'Ticker'],
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field='Data'
        )
    )

    try:
        job = client.load_table_from_dataframe(
            dataframe=df,
            destination=table,
            job_config=job_config)
        job.result()  # czekaj na zakończenie
        print(f"Data successfully inserted into {table}.")
    except Exception as e:
        print(f"Failed to insert data into BigQuery: {e}")
