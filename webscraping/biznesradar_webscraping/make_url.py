from urllib.parse import urljoin
import re
from biznesradar_webscraping_dict import (market_dict, reports_dict, data_dict, period_dict)


def make_url(
        market: str = 'gpw',
        report_type: str = 'Raporty_finansowe',
        sub_report_type: str = 'Rachunek_zyskow_i_strat',
        measure: str = 'Przychody_ze_sprzedazy',
        period: str = 'Kwartalne'):


    """ Creating url """
    result_dict = {}
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

    """ Creating path """
    path = f"{sub_report_type_url}/{market_url},{period_url},{measure_url}"

    """ Joining base URL with custom part """
    url = urljoin(base_url, path)

    """ Assigning data to dict"""
    result_dict[url] = {'path': path,
                        'market': market,
                        'report_type': report_type,
                        'sub_report_type': sub_report_type,
                        'measure': measure,
                        'period': period}
    return result_dict
