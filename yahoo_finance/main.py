import os
import logging
import pendulum
from dotenv import load_dotenv
from fetch_instruments_data import fetch_data_from_bigquery
from transform_data import transform_data

load_dotenv()

# -- Defining logger config --
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

if __name__ == '__main__':

    # -- Reading environment variables from .env file --
    BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID")
    BQ_DATASET_INSTRUMENTS = os.getenv("BQ_DATASET_INSTRUMENTS")
    BQ_TABLE_INSTRUMENTS = os.getenv("BQ_TABLE_INSTRUMENTS")

    # -- Defining flags and variables --
    static_instrument_list = ['AMB.WA', 'PZU.WA']
    USE_STATIC_INSTRUMENTS = True   # --> Use data from 'static_instrument_list' instead of reading from BigQuery

    # -- Defining basic time period --
    start_date = pendulum.date(2026, 1, 7)
    end_date = pendulum.date(2026, 1, 9)
    #period_to_fetch = "1d"

    # -- Defining SQL query to fetch data from BigQuery --
    sql = f"""
    SELECT DISTINCT
        ticker,
        market
    FROM `{BQ_PROJECT_ID}.{BQ_DATASET_INSTRUMENTS}.{BQ_TABLE_INSTRUMENTS}`
    WHERE TRUE
        AND instrument_type_id = SAFE_CAST(@instrument_type_id AS INT64)
    """

    # -- Fetching instruments data from BigQuery --
    instruments_df = fetch_data_from_bigquery(sql, params={'instrument_type_id': 1})

    # -- Data Transformation --
    df_final = transform_data(instruments_df)

