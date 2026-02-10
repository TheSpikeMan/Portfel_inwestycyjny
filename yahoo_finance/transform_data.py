import pandas as pd
import logging


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Starting data transformation.")

    # -- Mapping markets to RIC suffixes --
    suffix_map = {
        'WSE': '.WA',
        'AEX': '.AS',
        'LSE': '.L',
        'FRA': '.DE',
        'PAR': '.PA'
    }
    df_with_suffixes = df.copy()
    df_with_suffixes['suffix'] = df['market'].map(suffix_map)

    # -- Defining tickers according to Reuters Instrument Codes (RIC) --
    df_with_suffixes['ticker_RIC'] = df_with_suffixes['ticker'] + df_with_suffixes['suffix']
    df_with_suffixes.drop(labels=['ticker', 'market', 'suffix'], axis=1, inplace=True)

    logging.info(f"Number of total tickers: {df_with_suffixes['ticker_RIC'].size}")
    logging.info(f"Number of correct tickers: {df_with_suffixes['ticker_RIC'].count()}")
    logging.info(f"Number of incorrect tickers: {df_with_suffixes['ticker_RIC'].isna().sum()}")

    # -- Removing null values --
    df_with_suffixes.dropna(axis=0, subset=['ticker_RIC'], inplace=True)

    logging.info("Data transformation finished.")

    return df_with_suffixes
