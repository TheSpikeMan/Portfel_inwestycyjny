# import bibliotek używanych w kodzie

import requests
import pandas as pd
import pandas_gbq
import functions_framework
from datetime import date
from google.cloud import bigquery
from flask import Flask, request


@functions_framework.cloud_event
def inflation_webscraping(cloud_event):

    # zdefiniowanie ścieżek do pobierania aktualnych kursów walut
    path1 = "http://api.nbp.pl/api/exchangerates/rates/A/USD/"
    path2 = "http://api.nbp.pl/api/exchangerates/rates/A/EUR/"
    
    list_of_paths = [path1, path2]
    df = pd.DataFrame()
    
    for path in list_of_paths:
        response = requests.get(path)

        data = response.json()
        currency_date = date.today()
        currency_code = data['code']
        currency_close = data['rates'][0]['mid']
        
        # Create a DataFrame with the extracted data
        currency_df = pd.DataFrame({
            'Currency_date': [currency_date],
            'Currency': [currency_code],
            'Currency_close': [currency_close]
        })
        
        # Concatenate the DataFrame to the main DataFrame
        df = pd.concat([df, currency_df], ignore_index=True)
                  
    # Sending the data to BQ
    client = bigquery.Client()
    
    project_id = 'projekt-inwestycyjny'
    dataset_id = 'Waluty'
    table_id = 'Currency'
    destination_table = f"{project_id}.{dataset_id}.{table_id}"
    
    try:
        df.to_gbq(destination_table, project_id=project_id, if_exists='append')
        print("Success exporting the data to BigQuery!")
        #return "Program zakończył się pomyślnie."
    except Exception as e:
        print(f"Error uploading data to BigQuery: {str(e)}")
        #return "Error"
        
# % Parametryzacja w Google Cloud Functions:
"""
Konfiguracja:
    Region: europe-central2 
    Typ aktywatora: Pub/Sub
    Pamięć przydzielona: 512 MiB
    CPU: 0.162
    Przekroczony limit czasu: 60
    Maksymalna liczba żądań na instancję: 1
    Minimalna liczba instancji: 0
    Maksymalna liczba instancji: 1
    Konto usługi srodowiska wykonawczego:
        Default Compute Service Account
    
Punkt wejscia:
    currencies_webscraping


Requirements:
    functions-framework==3.*
    bs4
    requests
    datetime
    pandas
    pandas_gbq
    google.cloud
"""

  
    
    
    
