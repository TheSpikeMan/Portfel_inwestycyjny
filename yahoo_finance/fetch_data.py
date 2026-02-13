import yfinance as yf
import pandas as pd


def fetch_data_from_yahoo_finance(
        tickers_list_to_fetch: list = None,
        tickers_dates_conf: dict = None
) -> pd.DataFrame | None:
    """
    :param
        tickers_list_to_fetch: list of tickers to fetch from Yahoo Finance
        tickers_dates_conf: time-configurating dict
    :return:
        result: DataFrame with data fetched
    """

    result = yf.download(
        tickers=tickers_list_to_fetch,
        group_by='ticker',
        **tickers_dates_conf
    )
    return result
