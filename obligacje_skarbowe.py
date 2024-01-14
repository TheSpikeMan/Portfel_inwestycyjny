# -*- coding: utf-8 -*-
"""
Created on Tue Jan  2 20:52:18 2024

@author: grzeg
"""

import pandas as pd
from datetime import date
from datetime import timedelta
import math
from google.cloud import bigquery
import numpy as np
import base64
import functions_framework
import pandas_gbq
from flask import Flask, request

@functions_framework.cloud_event
def Treasury_bonds_daily(cloud_event):
    project_id = 'projekt-inwestycyjny'
    dataset_id_1 = 'Inflation'
    table_id_1 = 'Inflation'
    destination_table_1 = f"`{project_id}.{dataset_id_1}.{table_id_1}`"
    
    dataset_id_2 = 'Transactions'
    table_id_2 = 'Transactions_view'
    destination_table_2 = f"`{project_id}.{dataset_id_2}.{table_id_2}`"
    
    dataset_id_3 = 'Dane_instrumentow'
    table_id_3 = 'Treasury_Bonds'
    destination_table_3 = f"`{project_id}.{dataset_id_3}.{table_id_3}`"
    
    query_1 = f"""
        SELECT
            inflation,
            date
        FROM {destination_table_1}
        WHERE TRUE
        """
        
    query_2 = f"""
        SELECT
            *
        FROM  {destination_table_2}
        WHERE
            Instrument_type_id = 5
        """
        
    query_3 = f"""
        SELECT
            *
        FROM  {destination_table_3}
        WHERE
            TRUE
        """
    
    client = bigquery.Client()
    query_job_1 = client.query(query_1)
    query_job_2 = client.query(query_2)
    query_job_3 = client.query(query_3)
    dane_inflacyjne = query_job_1.to_dataframe()
    dane_transakcyjne = query_job_2.to_dataframe()
    dane_marz = query_job_3.to_dataframe()
    
    dane_inflacyjne.columns = ['Inflacja', 'Początek miesiąca']
    dane_obligacji = dane_transakcyjne.merge(right=dane_marz, 
                                     how='inner', 
                                     on = 'Ticker')
    
        
    ticker_list = (dane_obligacji['Ticker'])
    dates_list = (dane_obligacji['Transaction_date'])
    wolumen_list = (dane_obligacji['Transaction_amount'])
    marza_pierwszy_rok_list = (dane_obligacji['First_year_interest'])
    marza_kolejne_lata_list = (dane_obligacji['Regular_interest'])
    
    dane_do_analizy = pd.concat([ticker_list, 
                       dates_list, 
                       wolumen_list,
                       marza_pierwszy_rok_list,
                       marza_kolejne_lata_list], 
                       axis=1)
    
    
    # analityka - szukanie wartosci aktualnej
    result_df = pd.DataFrame(columns=['Ticker', 'Date', 'Current Value'])
    
    for dane in dane_do_analizy.iterrows():
        ticker = dane[1].iloc[0]
        data_zakupu = dane[1].iloc[1]
        wolumen = dane[1].iloc[2]
        marza_pierwszy_rok = dane[1].iloc[3]
        marza_kolejne_lata = dane[1].iloc[4]
        
        # calculating the start_value
        wolumen_jednostkowy = 100
        start_value = wolumen * wolumen_jednostkowy
        
        # calculating current date, finding the end date, calculating the number of days and years
        current_date = date.today()
        liczba_dni = (current_date - data_zakupu).days
        liczba_lat = int(math.modf(liczba_dni/365)[1])
    
        # writing the code to find current value
        n = 1
        if liczba_dni < 365:
            current_value = start_value + start_value * liczba_dni / 365 * (marza_pierwszy_rok/100)
        
        else:
            current_value = start_value + start_value * (marza_pierwszy_rok/100)
            for i in range(liczba_lat, 0, -1):
                liczba_dni_przesuniecie = timedelta(days= 365 * n - 60)
                data_badania_inflacji = date((data_zakupu + liczba_dni_przesuniecie).year, \
                                     (data_zakupu + liczba_dni_przesuniecie).month, \
                                     1)
                inflacja = dane_inflacyjne.loc[dane_inflacyjne['Początek miesiąca'] \
                                               == str(data_badania_inflacji)].iat[0,0]
    
                if liczba_dni < 730:
                    current_value = current_value + current_value * \
                        (liczba_dni - 365)/365 * (inflacja + marza_kolejne_lata)/ 100
                else:
                    current_value = current_value + current_value * \
                        (inflacja + marza_kolejne_lata) / 100
                    liczba_dni = liczba_dni - 365
                n = n + 1 
                
        result_df = pd.concat([result_df, \
                               pd.DataFrame(data=[[ticker, data_zakupu, \
                                                   round(current_value, 2)]], \
                                            columns=['Ticker', 'Date', 'Current Value'])])
        result_dfs = result_df.merge(right=dane_obligacji, 
                        how='inner',
                        left_on=['Ticker', 'Date'],
                        right_on= ['Ticker', 'Transaction_date'])
        data_to_export = result_dfs
        data_to_export['Date'] = current_date
        data_to_export['Close'] = (data_to_export['Current Value']/\
                                   data_to_export['Transaction_amount']).round(3)
        data_to_export = data_to_export.groupby(['Ticker', 'Date']).\
            apply(lambda x: np.average(x['Close'], \
            weights=x['Transaction_amount']))\
            .reset_index(name='Close').\
            round(decimals = 3)
        data_to_export['Volume'] = 0
        data_to_export['Turnover'] = 0
    
    
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
        
        # 1.5. Zdefiniowanie parametrów tabeli, do której pisze CF.
    project_id = 'projekt-inwestycyjny'
    dataset_id = 'Dane_instrumentow'
    table_id = 'Daily'
    destination_table = f"{project_id}.{dataset_id}.{table_id}"
      
    # 18. Wyznaczenie konfiguracji dla joba i wykonanie joba.
    job_config = bigquery.LoadJobConfig(schema = schema,
                                        write_disposition = "WRITE_APPEND")
    try:
        job = client.load_table_from_dataframe(data_to_export, 
                                               destination_table,
                                               job_config = job_config)
        job.result()
    except Exception as e:
        print(f"Error uploading data to BigQuery: {str(e)}")
        return "Błąd eksportu danych do BigQuery."
    
    print("Dane obligacji skarbowych zostały przekazane do tabeli BigQuery.")
    return "Program zakończył się pomyślnie."


"""
Konfiguracja:
Region: europe-central2 
Typ aktywatora: Pub/Sub
Pamięć przydzielona: 256 MiB
CPU: 0.167
Przekroczony limit czasu: 60
Maksymalna liczba żądań na instancję: 1
Minimalna liczba instancji: 0
Maksymalna liczba instancji: 1
Konto usługi srodowiska wykonawczego:
Default Compute Service Account

Punkt wejscia:
obligacje_skarbowe


Requirements:
functions-framework==3.*
datetime
numpy
pandas
pandas_gbq
google.cloud
"""