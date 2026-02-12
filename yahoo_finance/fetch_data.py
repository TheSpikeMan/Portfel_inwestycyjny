import yfinance as yf
import pandas as pd

def fetch_data_from_yahoo_finance(
        tickers_list_to_fetch: list = None,
        tickers_dates_conf: dict = None
) -> pd.DataFrame | None:
    """
    :param
        tickers_list_to_fetch: list of ticker to fetch from Yahoo Finance
        tickers_start_date: starting date to fetch data
        tickers_end_date: end date to fetch data
        tickers_period: period to fetch data
    :return:
    """

    result = yf.download(
        tickers=tickers_list_to_fetch,
        group_by='ticker',
        **tickers_dates_conf
    )
    return result
