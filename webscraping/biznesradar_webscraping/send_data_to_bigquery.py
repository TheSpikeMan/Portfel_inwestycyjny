import os
import logging
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

""" Logging """
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

""" Pobieram zmienne środowiskowe """
load_dotenv()

PROJECT_ID = os.getenv("BQ_PROJECT_ID")
DATASET_ID = os.getenv("BQ_DATASET_RAW")
TABLE_ID = os.getenv("BQ_TABLE_RAW_DATA")

if not all([PROJECT_ID, DATASET_ID, TABLE_ID]):
    raise ValueError("Missing BigQuery environment variables")

""" Inicjalizacja klienta BigQuery """
bq_client = bigquery.Client()
TABLE_FULL_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"


def send_data_to_bigquery(df: pd.DataFrame) -> None:
    """
    Batchowa wysyłka danych do BigQuery.
    Forma zapisu danych: append.
    """

    """ Weryfikuję zawartość DataFrame """
    if df.empty:
        logger.info("Empty DataFrame - skipping BigQuery load.")
        return

    """ Konfiguracja joba BigQuery """
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        clustering_fields=['Timestamp', 'Ticker'],
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field='Data'
        )
    )

    try:
        job = bq_client.load_table_from_dataframe(
            dataframe=df,
            destination=TABLE_FULL_ID,
            job_config=job_config)
        job.result()  # czekaj na zakończenie
        logger.info(f"Data successfully inserted into {TABLE_FULL_ID}.")
    except GoogleAPIError:
        logger.exception("BigQuery API error.")
        raise
    except Exception as e:
        logger.exception(f"Unexpected error during BigQuery load: {e}.")
        raise
