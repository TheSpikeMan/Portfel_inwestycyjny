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


def send_data_to_bigquery(df: pd.Dataframe, destination_params: dict = None):
    """
    :param
        df: DataFrame to send to BigQuery table
        destination_params: dictionary with destination parameters
    :return: None
    """

    # -- DataFrame and dictionary validation part --
    if df.empty:
        logging.error("DataFrame is empty. Skipping export.")
        return

    missing_params = [name for name, value in destination_params.items() if value is None]
    if missing_params:
        raise ValueError("Destination parameters are missing: {', '.join(missing_params)} ")

    # -- Basic transformations and declarations --
    df.columns = [c.strip().replace(' ', '_').lower() for c in df.columns]

    # -- Assigning parameters to variables --
    project_name = destination_params.get('project_name')
    dataset_name = destination_params.get('dataset_name')
    table_name = destination_params.get('table_name')
    location_name = destination_params.get('location_name')
    destination_table = f"{project_name}.{dataset_name}.{table_name}"

    # -- Starting sending data process --
    client = bigquery.Client(project=destination_params['project_name'])
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        autodetect=True,
        labels={
            'module': 'webscraping',
            'routine': 'ad-hoc'
        },
    )

    # -- Sending data to BigQuery --
    try:
        load_job = client.load_table_from_dataframe(
            dataframe=df,
            destination=destination_table,
            location=f"{location_name}",
            job_config=job_config
        )
        load_job.result()
    except Exception as e:
        logging.error(f"Failed to load data to BigQuery: {e}")
        raise
