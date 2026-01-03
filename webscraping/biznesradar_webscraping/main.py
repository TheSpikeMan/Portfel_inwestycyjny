from make_url import make_url
from make_request import make_request
from transform_data import transform_data
from biznesradar_webscraping_dict import market_dict, reports_dict, data_dict, period_dict
import pandas as pd

if __name__ == '__main__':

    """ 
    Definining restrictions in webscraping 
    FIELDS TO FILL IF RESTRICTIONS ARE TO BE SET
    """

    """ f.e. 'gpw' """
    market_restrictions = []
    """ f.e. 'Wskazniki' """
    report_group_restrictions = []
    """ f.e. 'Rachunek_zyskow_i_strat' """
    reports_restrictions = []
    """ f.e. 'Przychody_ze_sprzedazy' """
    report_detailed_restrictions = []
    """ f.e. 'Kwartalne' """
    period_restrictions = []

    restrictions = {
        'market_restrictions': market_restrictions,
        'report_group_restrictions': report_group_restrictions,
        'report_restrictions': reports_restrictions,
        'report_detailed_restrictions': report_detailed_restrictions,
        'period_restrictions': period_restrictions
    }

    """ Creating urls to request """
    url_dict_to_request = make_url(restrictions)

    """ Creating requests to website"""
    make_request(url_dict_to_request)
