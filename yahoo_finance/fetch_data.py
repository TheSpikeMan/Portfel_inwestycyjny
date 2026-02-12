import yfinance as yf
import pandas as pd
from datetime import date
import pendulum


def fetch_data_from_yahoo_finance(
        tickers_list_to_fetch: list = None,
        tickers_start_date: date | None = pendulum.datetime(2026, 1, 1),
        tickers_end_date: date | None = pendulum.now(),
        #tickers_period: str | None = "1d"
) -> pd.DataFrame | None:
    """
    :param
        tickers_list_to_fetch: list of ticker to fetch from Yahoo Finance
        tickers_start_date: starting date to fetch data
        tickers_end_date: end date to fetch data
        #tickers_period: period to fetch data
    :return:
    """

    result = yf.download(
        tickers=tickers_list_to_fetch,
        start=tickers_start_date,
        end=tickers_end_date,
        group_by='ticker'
        #period=tickers_period
    )
    return result
