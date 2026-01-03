from make_url import make_url
from make_request import make_request
from transform_data import transform_data
from biznesradar_webscraping_dict import market_dict, reports_dict, data_dict, period_dict

if __name__ == '__main__':
rows = []
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

                # 3. Potrójna pętla: dla każdego parametru i KAŻDEGO okresu tworzymy osobny wiersz
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
""" Creating DataFame"""
df = pd.DataFrame(rows)

""" Creating urls to request """
urls_to_request = make_url(df)

""" Creating requests to website"""
make_request(urls_to_request)