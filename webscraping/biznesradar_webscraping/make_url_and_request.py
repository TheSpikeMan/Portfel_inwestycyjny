import requests
from urllib.parse import urljoin
import re
from biznesradar_webscraping_dict import (market_dict, reports_dict, data_dict, period_dict)


def make_url_and_request(
        market: str = 'gpw',
        report_type: str = 'Raporty_finansowe',
        sub_report_type: str = 'Rachunek_zyskow_i_strat',
        measure: str = 'Przychody_ze_sprzedazy',
        period: str = 'Kwartalne'):

    """ Creating url """
    base_url = 'https://www.biznesradar.pl/'
    sub_report_type_url = [sub_report_type_instance.get(sub_report_type)
                           for sub_report_type_instance
                           in reports_dict.get(report_type)
                           if sub_report_type_instance.get(sub_report_type)][0]

    market_url = market_dict.get(market)
    measure_url = [measure_instance.get(measure)
                   for measure_instance
                   in data_dict.get(sub_report_type)
                   if measure_instance.get(measure)][0]
    period_url = [period_instance.get(period)
                  for period_instance
                  in period_dict.get('Okres')
                  if period_instance.get(period)][0]

    path = f"{sub_report_type_url}/{market_url},{period_url},{measure_url}"
    url = urljoin(base_url, path)

    """ Creating request"""
    try:
        with requests.request(method='GET', url=url) as r:
            r.raise_for_status()
            if r.status_code == 200:
                data = r.text
                return data, path
    except ConnectionError:
        print("Connection error")
        return (0,0)