import requests
import time
import random
from transform_data import transform_data

def make_request(urls_list: list):
    """ Creating request"""
    try:
        with requests.session() as s:
            for list_item in urls_list:
                for url, params_list in list_item.items():
                    # Pobranie zmiennych ze słownika z parametryzacją
                    path = next(iter(params_list)).get('path')
                    report_name = next(iter(params_list)).get('report_name')
                    r = s.get(url=url)
                    wait_time = random.randint(10, 20)
                    time.sleep(wait_time)
                    r.raise_for_status()
                    if r.status_code == 200:
                        data = r.text
                        transform_data(data, path, report_name)
    except ConnectionError:
        print("Connection error")
