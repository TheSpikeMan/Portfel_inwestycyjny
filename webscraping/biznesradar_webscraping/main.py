from make_url import make_url
from make_request import make_request
from transform_data import transform_data
from biznesradar_webscraping_dict import market_dict, reports_dict, data_dict, period_dict

if __name__ == '__main__':
    """ Creating URLS to request """
    data_to_build_urls = []
    urls_to_request = []
    market = 'gpw'
    report_type = 'Raporty_finansowe'
    sub_report_type = 'Rachunek_zyskow_i_strat'
    for subreport_type in data_dict.get(sub_report_type):
        sub_report_type_url = next(iter(subreport_type.keys()))
        for period_instance in period_dict.get('Okres'):
            period_instance_url = next(iter(period_instance.keys()))
            data_to_build_urls.append(
                {'market': market,
                 'report_type': report_type,
                 'sub_report_type': sub_report_type,
                 'measure': sub_report_type_url,
                 'period': period_instance_url}
            )
    for data_to_build_url in data_to_build_urls:
        urls_to_request.append(make_url(**data_to_build_url))

    make_request(urls_to_request)
