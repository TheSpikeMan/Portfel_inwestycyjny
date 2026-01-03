import requests
import time
import random
from transform_data import transform_data
from send_data_to_bigquery import send_data_to_bigquery
import pandas as pd

def make_request(urls_list: list[dict]):

    BATCH_SIZE = 10
    all_dfs = []
    index = 1

    """ Creating request"""

    with requests.session() as s:
        logger.info("Rozpoczynam pobieranie danych.")
        for url, params_dict in urls_dict.items():
            logger.info(f"Trwa pobieranie danych: {index}/{len(urls_dict)}.")
            index += 1

            try:
                r = s.get(url=url)
                r.raise_for_status()
            except requests.RequestException as e:
                logger.error(f"Request failed for {url}: {e}")
                continue

            data = r.text
            df = transform_data(data, params_dict)
            all_dfs.append(df)

            if len(all_dfs) >= BATCH_SIZE:
                df_batch = pd.concat(all_dfs, ignore_index=True)
                send_data_to_bigquery(df_batch)
                all_dfs.clear()
            time.sleep(random.randint(5, 10))

    if all_dfs:
        df_batch = pd.concat(all_dfs, ignore_index=True)
        send_data_to_bigquery(df_batch)


