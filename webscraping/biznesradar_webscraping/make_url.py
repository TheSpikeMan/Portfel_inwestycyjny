from urllib.parse import urljoin
import re
from biznesradar_webscraping_dict import (BASE_URL, market_dict, reports_dict, data_dict, period_dict)
import pandas as pd


def make_url(list_of_restrictions: dict) -> dict:
    """
    :params: list of restrictions to scraping
    :return: dict, where KEY is URL, and VALUE are parameters describing key as an internal dict
    """

    """ Declaring variables """
    rows = []
    results_dict = {}

    """ Iterating over markets """
    for market_id, market_val in market_dict.items():
        """ Iterating over report groups """
        for group_name, reports_list in reports_dict.items():
            """ Iterating over reports"""
            for report_item in reports_list:
                """ Iterating over report mappings"""
                for report_type, report_slug in report_item.items():
                    """ Fetching inital parameter - report type, f.e 'Rachunek_zyskow_i_strat' """
                    lookup_key = report_type

                    """ Fetching list of reports for chosen report """
                    subreports = data_dict.get(lookup_key, [])

                    """ Fetching available periods for chosen report """
                    periods = period_dict.get(report_type, {})

                    """ Declaring periods dict if there are any items and empty one if not """
                    period_items = periods.items() if periods else [('', '')]

                    """ Iterating over report """
                    for sub_item in subreports:
                        """ Iterating over every report dict """
                        for sub_name, sub_code in sub_item.items():
                            """ Iterating over every period dict """
                            for p_name, p_code in period_items:
                                rows.append({
                                    "market": market_id,
                                    "market_slug": market_val,
                                    "report_group": group_name,
                                    "report": report_type,
                                    "raport_slug": report_slug,
                                    "report_detailed": sub_name,
                                    "report_detailed_slug": sub_code,
                                    "period": p_name,
                                    "period_slug": p_code
                                })
    """ Creating DataFame with full data """
    df = pd.DataFrame(rows)

    """ Creating a copy of DataFrame to work with """
    df_work = df.copy()

    """ Applying restrictions from main.py lists"""
    market_mask = list_of_restrictions.get('market_restrictions', [])
    report_group_mask = list_of_restrictions.get('report_group_restrictions', [])
    report_mask = list_of_restrictions.get('report_restrictions', [])
    report_detailed_mask = list_of_restrictions.get('report_detailed_restrictions', [])
    period_mask = list_of_restrictions.get('period_restrictions', [])

    df_work = df_work[
        ~df_work['market'].isin(market_mask) &
        ~df_work['report_group'].isin(report_group_mask) &
        ~df_work['report'].isin(report_mask) &
        ~df_work['report_detailed'].isin(report_detailed_mask) &
        ~df_work['period'].isin(period_mask)
        ]

    """ Creating path and url """
    df_work['path'] = df_work['raport_slug'] + '/' + df_work['market_slug'] + ',' + df_work['period_slug'] + ',' + \
        df_work['report_detailed_slug']
    df_work['url'] = BASE_URL + df_work['path']

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
