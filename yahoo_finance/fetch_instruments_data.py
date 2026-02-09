from google.cloud import bigquery
import pandas as pd
import logging


def fetch_data_from_bigquery(sql: str, params: dict = None):
    """
    Fetching data from BigQuery with errors handling
    :param sql: String with SQL query (use @param_name for variables)
    :param params: Dictionary with parameters, fe. {'ticker': 'ACP.WA'}
    """
    client = bigquery.Client()

    # -- Configuring parameters --
    job_config = bigquery.QueryJobConfig()
    if params:
        query_params = [
            bigquery.ScalarQueryParameter(name, "STRING", value)
            for name, value in params.items()
        ]
        job_config.query_parameters = query_params

    try:
        logging.info("Launching BigQuery query...")
        query_job = client.query(sql, job_config=job_config)

        # -- Waiting for the result and converting to DataFrame --
        df = query_job.to_dataframe()

        if df.empty:
            logging.warning("Query return no results.")

        return df

    except Exception as e:
        logging.error(f"Error while fetching data: {e}")
        return pd.DataFrame()