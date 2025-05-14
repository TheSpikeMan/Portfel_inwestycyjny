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
async def get_latest_prices_by_ticker(ticker: str, limit: int = 30):
    query = f"""
        SELECT 
            Date, 
            Close
        FROM `{PROJECT_ID}.{DATASET_INSTRUMENTS}.{TABLE_DAILY}`
        WHERE TRUE
            AND AND Ticker = @ticker
        ORDER BY `Date` DESC
        LIMIT {limit}
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("ticker", "STRING", ticker)]
    )

    try:
        query_job = client.query(query, job_config=job_config)
        results = query_job.result()
        
        return [{"date": row["Date"], "price": row["Close"]} for row in results]
    
    except Exception as e:
        raise Exception(f"Error while fetching data: {str(e)}")

# Pobierz ostatni dostępny kurs dla danego instrumentu
async def get_last_price_by_ticker(ticker: str):

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("ticker", "STRING", ticker)]
    )

    query = f"""
        SELECT Close
        FROM `{PROJECT_ID}.{DATASET_INSTRUMENTS}.{TABLE_DAILY}
        WHERE TRUE
            AND Ticker = @ticker
        QUALIFY TRUE
            AND ROW_NUMBER() OVER ticker_window = 1
        WINDOW
            ticker_window AS (
                PARTITION BY
                    Ticker
                ORDER BY
                    `Date` DESC
                )
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("ticker", "STRING", ticker)
        ]
    )

    try:
        query_job = client.query(query, job_config=job_config)
        results = query_job.result()
        
        if results.total_rows == 0:
            return None  # Brak wyników
        
        row = list(results)[0]
        return row["Close"]

    except Exception as e:
        raise Exception(f"Error while fetching data: {str(e)}")