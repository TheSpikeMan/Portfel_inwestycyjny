import requests
import time
import random
from transform_data import transform_data
import pandas as pd

def make_request(urls_list: list):

    all_dfs = []
    """ Creating request"""
    try:
        with requests.session() as s:
            print("Rozpoczynam pobieranie danych.")
            index = 1
            for list_item in urls_list:
                for url, params_list in list_item.items():
                    # Pobranie zmiennych ze słownika z parametryzacją
                    print(f"Trwa pobieranie danych: {index}/{len(urls_list)}.")
                    index += 1
                    path = next(iter(params_list)).get('path')
                    report_name = next(iter(params_list)).get('report_name')
                    r = s.get(url=url)
                    wait_time = random.randint(5, 10)
                    time.sleep(wait_time)
                    r.raise_for_status()
                    if r.status_code == 200:
                        data = r.text
                        df = transform_data(data, path, report_name)
                        all_dfs.append(df)
        df_total = pd.concat(all_dfs, ignore_index=True)
        print("Rozpoczynam generowanie zbiorczego raportu...")
        df_total.to_excel("Raport_z_biznesradar.xlsx")
        print("Raport wygenerowany.")

    except ConnectionError:
        print("Connection error")
