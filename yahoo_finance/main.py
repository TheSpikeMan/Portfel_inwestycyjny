import os
import logging
import pendulum
from dotenv import load_dotenv
from bigquery_handler import fetch_data_from_bigquery, send_data_to_bigquery
from data_transform import transform_fetched_bigquery_data
from yfinance_provider import fetch_data_from_yahoo_finance

load_dotenv()

# -- Defining logger config --
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

if __name__ == '__main__':
    # ---------- USER PARAMETERS PART ----------
    # -- Reading environment variables from .env file --
    BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID")
    BQ_DATASET_INSTRUMENTS = os.getenv("BQ_DATASET_INSTRUMENTS")
    BQ_TABLE_INSTRUMENTS = os.getenv("BQ_TABLE_INSTRUMENTS")

    # -- Defining flags and variables --
    static_instrument_list = ['AMB.WA', 'PZU.WA']
    USE_STATIC_INSTRUMENTS = True   # --> Use static instruments instead of BigQuery source
    USE_PERIOD = False                # --> Use period instead of dates

    # -- Defining basic time period --
    # Define either start_date and end_date or period to fetch
    start_date = pendulum.date(2026, 1, 7)
    end_date = pendulum.date(2026, 1, 30)
    period_to_fetch = "1d"

    # -- Defining export destination dictionary --
    destination_bigquery = {
        'project_name': 'projekt-inwestycyjny',
        'dataset_name': 'Dane_instrumentow',
        'table_name': 'Daily_yfinance',
        'location_name': 'europe-central2'
    }

    # -- Defining SQL query to fetch data from BigQuery --
    sql = f"""
    SELECT DISTINCT
        ticker,
        market
    FROM `{BQ_PROJECT_ID}.{BQ_DATASET_INSTRUMENTS}.{BQ_TABLE_INSTRUMENTS}`
    WHERE TRUE
        AND instrument_type_id = SAFE_CAST(@instrument_type_id AS INT64)
    """
    # ---------- END OF USER PARAMETERS PART ----------

    # -- Fetching instruments data from BigQuery --
    instruments_df = fetch_data_from_bigquery(sql, params={'instrument_type_id': 1})

    # -- Data Transformation and USE_STATIC_INSTRUMENTS flag validation --
    resulting_tickers = static_instrument_list \
        if USE_STATIC_INSTRUMENTS \
        else transform_fetched_bigquery_data(instruments_df)

    # -- Dates config validation ---
    if USE_PERIOD:
        dates_params = {'period': period_to_fetch}
    else:
        dates_params = {'start': start_date,
                        'end': end_date}

    # -- Yahoo Finance data fetching --
    result_df = fetch_data_from_yahoo_finance(tickers_list_to_fetch=resulting_tickers,
                                              tickers_dates_conf=dates_params)

    # -- Yahoo Finance data transformation --
    result_df = (result_df.stack(level=0, future_stack=True)
                 .reset_index()
                 .drop(labels=['Open', 'High', 'Low', 'Volume'], axis=1))
    result_df['Ticker'] = result_df['Ticker'].str.split(pat='.').str[0]
    result_df['Close'] = result_df['Close'].round(decimals=2)

    # -- Sending data to BigQuery table --
    send_data_to_bigquery(df=result_df,
                          destination_params=destination_bigquery)
