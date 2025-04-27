from bs4 import BeautifulSoup
import requests
import pandas as pd
from datetime import date
from google.cloud import bigquery
import base64
import functions_framework
import pandas_gbq
from flask import Flask, request

class ETFScraper():
    
    def __init__(self, 
                 project, 
                 dataset_currency, 
                 dataset_instruments,
                 table_currencies,
                 table_instruments,
                 table_instruments_types,
                 table_daily):
        """

        Parameters
        ----------
        project : STRING - projekt, z którego pobierane są aktualne kursy walut
        dataset_currency : STRING - dataset, z którego pobierane są 
            aktualne kursy walut
        dataset_instruments : STRING - dataset, zawierający dane aktualnych 
            instrumentów
        table_currencies : STRING - tabela, z której pobierane są aktualne kursy walut
        table_instruments : STRING - tabela, zawierająca dane instrumentów
        table_instruments_types : STRING - tabela, zawierająca dane 
            typów instrumentów
        table_daily : STRING - tabela, zawierająca dane giełdowe instrumentów.
            Jest to tabela docelowa dla eksportów.

        Returns
        -------
        None.

        """
        self.project = project
        self.dataset_currency = dataset_currency
        self.dataset_instruments = dataset_instruments
        self.table_currencies = table_currencies
        self.table_instruments = table_instruments
        self.table_instruments_types = table_instruments_types
        self.table_daily = table_daily
        
        
    def pobierz_aktualne_kursy_walut(self):
        
        print("Pobieram aktualne kursy walut.")
        client = bigquery.Client()
        query = f"""
        WITH
        all_currencies_data_ordered AS (
          SELECT
            *,
            ROW_NUMBER() OVER(PARTITION BY Currency ORDER BY Currency_date DESC) as row_number
          FROM `{self.project}.{self.dataset_currency}.{self.table_currencies}`
          WHERE TRUE
        )
        
        SELECT *
        FROM all_currencies_data_ordered
        WHERE row_number = 1
                
        """
        
        query_job = client.query(query = query)
        return query_job.to_dataframe()
    
    def pobierz_aktualne_instrumenty_ETF(self):
        
        print("Pobieram aktualne instrumenty w ramach ETF.")
        query_2 = f"""
        SELECT
          Ticker,
          Market,
          Currency,
          Instrument_type
        FROM `{self.project}.{self.dataset_instruments}.{self.table_instruments}` AS inst
        LEFT JOIN `{self.project}.{self.dataset_instruments}.{self.table_instruments_types}` AS inst_typ
        ON inst.Instrument_type_id = inst_typ.Instrument_type_id
        WHERE Status = 1
        AND Instrument_type = 'ETF akcyjne zagraniczne'
        """
        client = bigquery.Client()
        query_job_2  = client.query(query = query_2)
        return query_job_2.to_dataframe()
        
    
    def webscraping_ETFs(self,
                         present_instruments,
                         present_currencies):
        
        print("Dokonuję webscrapingu dla wybranych instrumentów ETF.")
        result_df = pd.DataFrame()
        current_date = date.today()
        for instrument in present_instruments.iterrows():

            url = "https://markets.ft.com/data/etfs/tearsheet/summary?s=" + \
                    f"{instrument[1]['Ticker']}:" + \
                    f"{instrument[1]['Market']}:" + \
                    f"{instrument[1]['Currency']}"
                    
            with requests.get(url=url) as r:
                if r.status_code == 200:
                    print(f"Podłączono się do strony - {instrument[1]['Ticker']}.")
                
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
        return data_to_export
        
    
    def run_scraper(self):
        present_currencies = self.pobierz_aktualne_kursy_walut()
        present_instruments = self.pobierz_aktualne_instrumenty_ETF()
        data_to_export = self.webscraping_ETFs(present_instruments, present_currencies)
        exporter = BigQueryExporter(self.project, 
                                    self.dataset_instruments,
                                    self.table_daily)
        exporter.eksport_danych_do_BigQuery(data_to_export)
        
class BigQueryExporter():
    
    def __init__(self, project, dataset_to_export, table_to_export):
        """

        Parameters
        ----------
        project : STRING - projekt, do którego eksportowane są dane giełdowe
        dataset_to_export : STRING - dataset, do którego eksportowane są dane giełdowe
        table_to_export : STRING - tabela, do której eksportowane są dane giełdowe

        Returns
        -------
        None.

        """
        self.project = project
        self.dataset_to_export = dataset_to_export
        self.table_to_export = table_to_export
    
    def eksport_danych_do_BigQuery(self, data_to_export):
        print("Eksportuję dane do BigQuery.")
        client = bigquery.Client()
        destination_table = f"{self.project}.{self.dataset_to_export}.{self.table_to_export}"
        
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
        
        job_config = bigquery.LoadJobConfig(schema = schema,
                                            write_disposition = "WRITE_APPEND")

        try:
            job = client.load_table_from_dataframe(data_to_export, 
                                                   destination_table,
                                                   job_config = job_config)
            job.result()
            print("Dane zagranicznych ETF zostały przekazane do tabeli BigQuery.")
        except Exception as e:
            print(f"Error uploading data to BigQuery: {str(e)}")

@functions_framework.cloud_event
def ETFs_daily(cloud_event): 
    """

    Parameters
    ----------
    cloud_event : PubSub Event - Wiadomosc generowana przez Cloud Schedulera,
        wg harmonogramu.

    Returns
    -------
    None.

    """
    
    project = 'projekt-inwestycyjny'
    dataset_currency = 'Waluty'
    dataset_instruments = 'Dane_instrumentow'
    table_currencies = 'Currency'
    table_instruments = 'Instruments'
    table_instruments_types = 'Instrument_types'
    table_daily = 'Daily'
    
    scraper = ETFScraper(project, 
                        dataset_currency, 
                        dataset_instruments, 
                        table_currencies, 
                        table_instruments,
                        table_instruments_types, 
                        table_daily)
    
    scraper.run_scraper()