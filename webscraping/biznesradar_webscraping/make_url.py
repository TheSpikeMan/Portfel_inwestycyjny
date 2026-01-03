from urllib.parse import urljoin
import re
from biznesradar_webscraping_dict import (market_dict, reports_dict, data_dict, period_dict)
import pandas as pd


def make_url(df: pd.DataFrame) -> dict:
    """
    :param df: DataFrame with all mapping dictionary
    :return: list of dicts
    """

    """ Creating a copy of DataFrame to work with"""
    df_work = df.copy()

    """ Creating path and url """
    results_dict = {}
    base_url = 'https://www.biznesradar.pl/'

    df_work['path'] = df_work['raport_slug'] + '/' + df_work['market_slug'] + ',' + df_work['period_slug'] + ',' + \
        df_work['report_detailed_slug']
    df_work['url'] = base_url + df_work['path']

    for row in df_work.itertuples(index=False):
        results_dict[row.url] = {
                'path': row.path,
                'market': row.market,
                'report_group': row.report_group,
                'report': row.report,
                'report_detailed': row.report_detailed,
                'period': row.period
            }

    return results_dict
