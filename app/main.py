from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
import os
from google.cloud import bigquery
from fastapi import HTTPException

# Inicjalizacja aplikacji FastAPI
app = FastAPI()

# Montowanie folderu 'static' na ścieżce '/static'
app.mount("/static", StaticFiles(directory="static"), name="static")

client = bigquery.Client()

# Endpoint do strony głównej
@app.get("/", response_class=HTMLResponse)
async def get_home():
    # Zwrócenie pliku index.html z folderu static
    with open(os.path.join("static", "index.html")) as f:
        return HTMLResponse(content=f.read())

# Endpoint do pobierania ceny na podstawie ticker
@app.get("/prices")
async def get_price(ticker: str):
    print(f"Received ticker: {ticker}")  # Sprawdzanie wartości tickera

    # Zapytanie do BigQuery
    query = f"""
        SELECT
            Close
        FROM `projekt-inwestycyjny.Dane_instrumentow.Daily`
        WHERE Ticker = '{ticker}'
            AND Date = CURRENT_DATE() - 1
    """

    try:
        # Wykonanie zapytania
        query_job = client.query(query)
        results = query_job.result()

        # Sprawdzenie, czy wyniki są puste
        if results.total_rows == 0:
            raise HTTPException(status_code=404, detail="Ticker not found")

        # Pobranie pierwszego wyniku
        row = list(results)[0]
        price = row["Close"]  # Sprawdzamy, jaka kolumna zawiera cenę

        # Zwrócenie wyniku w formie JSON
        return {"ticker": ticker, "price": price}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error while fetching data: {str(e)}")
