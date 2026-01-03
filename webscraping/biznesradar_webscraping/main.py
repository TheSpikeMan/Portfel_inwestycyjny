from make_url import make_url
from make_request import make_request
from transform_data import transform_data
from biznesradar_webscraping_dict import market_dict, reports_dict, data_dict, period_dict
import pandas as pd

if __name__ == '__main__':

    """ Creating urls to request """
    url_dict_to_request = make_url()

    """ Creating requests to website"""
    make_request(url_dict_to_request)