from fastapi import APIRouter
from services import bigquery

router = APIRouter()

@router.get("/prices/{ticker}")
def get_price(ticker: str):
    return bigquery.get_last_price_by_ticker(ticker)

@router.get("/history/{ticker}")
def get_history(ticker: str):
    return bigquery.get_latest_prices_by_ticker(ticker)