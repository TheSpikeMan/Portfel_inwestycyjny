# import bibliotek używanych w kodzie

import requests
import pandas as pd
import pandas-gbq
import functions_framework
from datetime import date
from google.cloud import bigquery
from flask import Flask, request


@functions_framework.cloud_event
def currencies_webscraping(cloud_event):

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

    
    
    
    
