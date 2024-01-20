# -*- coding: utf-8 -*-
"""
Created on Wed Jan 17 17:38:44 2024

@author: grzeg
"""

from bs4 import BeautifulSoup
import requests
import pandas as pd
from datetime import date
from google.cloud import bigquery


# Wyznaczenie aktualnej daty
current_date = date.today()

# Dane do wyciągnięcia kursów ostatnich walut
project = 'projekt-inwestycyjny'
dataset = 'Waluty'
table = 'Currency'

# Dane do wyciągnięcia informacji o posiadanych aktualnie instrumentach
dataset_2 = 'Dane_instrumentow'
table_2_1 = 'Instruments'
table_2_2 = 'Instrument_types'

# 1. SQL pobiera aktualny kurs walut.
client = bigquery.Client()
query = f"""
WITH
all_currencies_data_ordered AS (
  SELECT
    *,
    ROW_NUMBER() OVER(PARTITION BY Currency ORDER BY Currency_date DESC) as row_number
  FROM `{project}.{dataset}.{table}`
  WHERE TRUE
)

SELECT *
FROM all_currencies_data_ordered
WHERE row_number = 1
        
"""

# SQL pobierający dane aktualnych instrumentow
query_2 = f"""
SELECT
  Ticker,
  Market,
  Currency,
  Instrument_type
FROM `{project}.{dataset_2}.{table_2_1}` AS inst
LEFT JOIN `{project}.{dataset_2}.{table_2_2}` AS inst_typ
ON inst.Instrument_type_id = inst_typ.Instrument_type_id
WHERE Status = 1
AND Instrument_type = 'ETF zagraniczne'
"""

query_job = client.query(query = query)
query_job_2 = client.query(query = query_2)
present_currencies = query_job.to_dataframe()
present_instruments = query_job_2.to_dataframe()
result_df = pd.DataFrame()


# 2. Webscrapingu zagranicznych ETFów

for instrument in present_instruments.iterrows():

    url = "https://markets.ft.com/data/etfs/tearsheet/summary?s=" + \
            f"{instrument[1]['Ticker']}:" + \
            f"{instrument[1]['Market']}:" + \
            f"{instrument[1]['Currency']}"
            
    with requests.get(url=url) as r:
        if r.status_code == 200:
            print('Connection to the site successful.')
        
        soup = BeautifulSoup(r.text, 'html.parser')
        
        close = soup.find_all('span', class_ = 'mod-ui-data-list__value')
        close = float(close[0].text)
        result_df = pd.concat([result_df, 
                               pd.DataFrame([[instrument[1]['Ticker'], close]])],
                               axis = 0)


result_df.columns = ['Ticker', 'Close']
data_to_export = present_instruments.merge(result_df,
                                           how = 'inner',
                                           on = 'Ticker')

data_to_export = data_to_export.merge(present_currencies,
                                      how = 'inner',
                                      on = 'Currency')
data_to_export['Close'] = (data_to_export['Close'] * \
                           data_to_export['Currency_close']).\
                           round(decimals = 2)
data_to_export['Date'] = current_date
data_to_export['Volume'] = 0 
data_to_export['Turnover'] = 0 

data_to_export = data_to_export.loc[:,['Ticker', 
                                       'Date', 
                                       'Close', 
                                       'Turnover', 
                                       'Volume']]


# Przygotowanie schematu danych w BigQuery, wg którego importowane będą dane.
schema = [bigquery.SchemaField(name = 'Ticker', field_type = "STRING", \
                               mode = "REQUIRED"),
          bigquery.SchemaField(name = 'Date', field_type = "DATE",\
                               mode = "REQUIRED"),
          bigquery.SchemaField(name = 'Close', field_type = "FLOAT",\
                               mode = "REQUIRED"),
          bigquery.SchemaField(name = 'Volume', field_type = "INTEGER",\
                               mode = "REQUIRED"),
          bigquery.SchemaField(name = 'Turnover', field_type = "INTEGER",\
                               mode = "NULLABLE")]
    
# Zdefiniowanie parametrów tabeli, do której pisze CF.
project_id = 'projekt-inwestycyjny'
dataset_id = 'Dane_instrumentow'
table_id = 'Daily'
destination_table = f"{project_id}.{dataset_id}.{table_id}"
  
# Wyznaczenie konfiguracji dla joba i wykonanie joba.
job_config = bigquery.LoadJobConfig(schema = schema,
                                    write_disposition = "WRITE_APPEND")

# Wykonanie operacji eksportu danych.
try:
    job = client.load_table_from_dataframe(data_to_export, 
                                           destination_table,
                                           job_config = job_config)
    job.result()
except Exception as e:
    print(f"Error uploading data to BigQuery: {str(e)}")

print("Dane zagranicznych ETF zostały przekazane do tabeli BigQuery.")
