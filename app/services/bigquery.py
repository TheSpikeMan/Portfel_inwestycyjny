from google.cloud import bigquery
import os
from dotenv import load_dotenv

# Wczytaj zmienne środowiskowe z .env
load_dotenv()

# Autoryzacja (np. przez ścieżkę do pliku serwisowego)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("BQ_CREDENTIALS_PATH")

# Inicjalizacja klienta BigQuery
client = bigquery.Client()

# Domyślne dane (zmień na swoje)
PROJECT_ID = os.getenv("BQ_PROJECT_ID")
DATASET_INSTRUMENTS = os.getenv("BQ_DATASET_INSTRUMENTS")
TABLE_DAILY = os.getenv("BQ_TABLE_DAILY")

# Pobierz ceny (np. 100 ostatnich rekordów) dla danego instrumentu
def get_latest_prices(limit=100):
    query = f"""
        SELECT 
            Date, 
            Close
        FROM `{PROJECT_ID}.{DATASET_INSTRUMENTS}.{TABLE_DAILY}`
        WHERE TRUE
            AND AND ticker = @ticker
        ORDER BY date DESC
        LIMIT {limit}
    """
    query_job = client.query(query)
    return [dict(row) for row in query_job]

# Pobierz ostatni dostępny kurs dla danego instrumentu
def get_price_by_ticker(ticker: str):
    query = f"""
        SELECT close
        FROM `{PROJECT_ID}.{DATASET_INSTRUMENTS}.{TABLE_DAILY}
        WHERE TRUE
            AND ticker = @ticker
        QUALIFY TRUE
            AND ROW_NUMBER() OVER ticker_window = 1
        WINDOW
            ticker_window AS (
                PARTITION BY
                    ticker
                ORDER BY
                    `Date` DESC
                )
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("ticker", "STRING", ticker)
        ]
    )
    query_job = client.query(query, job_config=job_config)
    rows = list(query_job)
    return dict(rows[0]) if rows else None