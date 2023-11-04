import base64
import functions_framework
from bs4 import BeautifulSoup
import requests
import datetime
import pandas as pd
import pandas_gbq
from google.cloud import bigquery
from flask import Flask, request

@functions_framework.cloud_event
def webscraping_investing_data(cloud_event):
  # Webscraping
  website_notowania ='https://www.biznesradar.pl/gielda/akcje_gpw'
  with requests.get(website_notowania) as r1:
    if r1.status_code == 200:
      print("Connection to website successful.")
      soup1 = BeautifulSoup(r1.text, 'html.parser')

      # finding all the 'tr' tags
      trs = soup1.find_all('tr')

      # getting all the tags classes
      trs_classes = [tr.get('class') for tr in trs]

      # joining the classes together and filtring out None values
      trs_classes = [" ".join(tr_class) for tr_class in trs_classes if tr_class != None]

      # removing the ad
      trs_classes.remove('ad')

      # finding the present date
      date = datetime.date.today()

      # creating an empty DataFrame
      result_df = pd.DataFrame()

      # Initialize a BigQuery client
      client = bigquery.Client()

      # Specify your SQL query
      query = """
      SELECT *
      FROM `projekt-inwestycyjny.Dane_instrumentow.Tickers`
      WHERE Status is True
      """

      # Run the query
      query_job = client.query(query)

      # Fetch and iterate over the results
      results = query_job.result()
      list_of_present_tickers = []
      for row in results:
        list_of_present_tickers.append(row.values()[0])

      #looking for all the tickers in the website
      dict_of_tickers = {}
      for index, tr_class in enumerate(trs_classes):
        Ticker = soup1.find('tr', class_ = tr_class ).find('a').get_text().split(' ')[0]
        dict_of_tickers.update({tr_class : Ticker})

      # finding the values that have status = 1 and saving the data to list
      list_of_trs_to_update = []
      for key, value in dict_of_tickers.items():
        if value in list_of_present_tickers:
          list_of_trs_to_update.append(key)

      # looping through all classes (instruments) with status = 1  and appending the data to DataFrame
      for index, tr_class in enumerate(list_of_trs_to_update):
        Ticker = soup1.find('tr', class_ = tr_class ).find('a').get_text().split(' ')[0]
        Date = datetime.date.today()
        Close = soup1.find('tr', class_ = tr_class).find('span', class_ = "q_ch_act").get_text(strip=True).replace(" ", "")
        if Close:
          Close = float(Close)
        else:
          continue
        Volume = soup1.find('tr', class_ = tr_class).find('span', class_ = "q_ch_vol").get_text(strip=True).replace(" ", "")
        if Volume:
          Volume = int(Volume)
        else:
          continue
        Turnover = soup1.find('tr', class_ = tr_class).find('span', class_ = "q_ch_mc").get_text(strip=True).replace(" ", "")
        if Turnover:
          Turnover = int(Turnover)
        else:
          continue
        instruments = [Ticker, Date, Close, Volume, Turnover]
        result_df = pd.concat([result_df, pd.DataFrame([instruments])], axis = 0)

      # changing the name of the columns in DataFrame
      result_df.columns = ['Ticker' , 'Date', 'Close', 'Volume', 'Turnover']

      # initializing BigQuery Client
      project_id = 'projekt-inwestycyjny'
      dataset_id = 'Dane_instrumentow'
      table_id = 'Daily'
      destination_table = f"{project_id}.{dataset_id}.{table_id}"
      
      # sending the data to BigQuery
      try:
        result_df.to_gbq(destination_table, project_id=project_id, if_exists='append')
        print("Success exporting the data to BigQuery!")
        return "Program zakończył się pomyślnie."
      except Exception as e:
        print(f"Error uploading data to BigQuery: {str(e)}")
        return "Error"
    else:
      print("Problem connecting to website.")
      return "Nie udało się połączyć"
    


