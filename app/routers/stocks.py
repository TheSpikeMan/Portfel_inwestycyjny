from fastapi import APIRouter, HTTPException
from services import bigquery

router = APIRouter()

@router.get("/prices/{ticker}")
async def get_price(ticker: str):
    price = await bigquery.get_last_price_by_ticker(ticker)
    if price is None:
        raise HTTPException(status_code=404, detail="Ticker not found")
    return {"ticker": ticker, "price": price}

@router.get("/history/{ticker}")
async def get_history(ticker: str, limit: int = 30):
    history = await bigquery.get_latest_prices_by_ticker(ticker, limit)
    if not history:
        raise HTTPException(status_code=404, detail="Ticker not found")
    return {"ticker": ticker, "history": history}