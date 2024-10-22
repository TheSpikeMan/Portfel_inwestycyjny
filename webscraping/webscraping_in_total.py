from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from google.cloud import bigquery
from datetime import datetime, date, timedelta
import requests
import math
import base64
import pandas_gbq
import functions_framework
from flask import Flask, request

@functions_framework.cloud_event
def daily_webscraping_plus_currencies(cloud_event):
    class Scraper(): 
        def __init__(self,
                    project_id,
                    dataset_instruments,
                    dataset_currencies,
                    dataset_daily,
                    dataset_inflation,
                    dataset_transactions,
                    table_instruments,
                    table_instruments_types,
                    table_currencies,
                    table_daily,
                    table_inflation,
                    table_treasury_bonds,
                    view_transactions,
                    website_stocks,
                    website_etfs_pl):
            
            """

            Inicjalizacja obiektu klasy Scraper i przypisanie do niego wartości zmiennych określonych przez użytkownika.

            """
        
            self.project_id              = project_id
            self.dataset_instruments     = dataset_instruments
            self.dataset_currencies      = dataset_currencies
            self.dataset_daily           = dataset_daily
            self.dataset_inflation       = dataset_inflation
            self.dataset_transactions    = dataset_transactions
            self.table_instruments       = table_instruments
            self.table_instruments_types = table_instruments_types
            self.table_currencies        = table_currencies
            self.table_daily             = table_daily
            self.table_inflation         = table_inflation
            self.table_treasury_bonds    = table_treasury_bonds
            self.view_transactions       = view_transactions
            self.website_stocks          = website_stocks
            self.website_etfs_pl         = website_etfs_pl

        def pobierz_aktualne_instrumenty(self):

            """
            Pobranie listy aktualnych instrumentów ETF - query_job_1.to_dataframe()
            Pobranie listy aktualnych instrumentów akcji polskich i ETF polskich - query_job_2.to_dataframe()
            """

            print("Pobieram aktualne instrumenty w ramach ETF oraz polskich akcji.")
            query_1 = f"""
            SELECT
            Ticker,
            Market,
            Currency,
            Instrument_type
            FROM `{self.project_id}.{self.dataset_instruments}.{self.table_instruments}` AS inst
            LEFT JOIN `{self.project_id}.{self.dataset_instruments}.{self.table_instruments_types}` AS inst_typ
            ON inst.Instrument_type_id = inst_typ.Instrument_type_id
            WHERE Status = 1
            AND Instrument_type = 'ETF zagraniczne'
            """

            query_2 = f"""
            SELECT
            Ticker,
            Status
            FROM `{self.project_id}.{self.dataset_instruments}.{self.table_instruments}` AS inst
            LEFT JOIN `{self.project_id}.{self.dataset_instruments}.{self.table_instruments_types}` AS inst_typ
            ON inst.Instrument_type_id = inst_typ.Instrument_type_id
            WHERE Status = 1
            AND inst.Instrument_type_id IN (1,3)
            """

            client = bigquery.Client()
            try:
                query_job_1 = client.query(query=query_1)
                query_job_2 = client.query(query=query_2)
                print("Pobieranie aktualnych instrumentów w portfelu inwestycyjnym zakończone powodzeniem.")
                return query_job_1.to_dataframe(), query_job_2.to_dataframe()
            except:
                print("Podczas pobierania danych instrumentów nastąpił błąd.")
        
        def znajdz_kursy_walut(self):

            """
            Znajdź aktualne kursy walut.
            Wykorzystywane są one do wyznaczenia wartość danego instrumentu w PLN.
            Eksportowane są w późniejszych krokach również do tabeli `Waluty.Currency`.
            """

            print("Szukam aktualnych kursów walut.")

            path1 = "http://api.nbp.pl/api/exchangerates/rates/A/USD/"
            path2 = "http://api.nbp.pl/api/exchangerates/rates/A/EUR/"

            list_of_paths = [path1, path2]
            aktualne_kursy_walut = pd.DataFrame()
            
            for path in list_of_paths:
                response       = requests.get(path)
            
                data = response.json()
                currency_date  = date.today()
                currency_code  = data['code']
                currency_close = data['rates'][0]['mid']
                
                currency_df = pd.DataFrame({
                    'Currency_date': [currency_date],
                    'Currency': [currency_code],
                    'Currency_close': [currency_close]
                    })
                
                aktualne_kursy_walut = pd.concat([aktualne_kursy_walut, currency_df], ignore_index=True)

            print("Poszukiwanie aktualnych kursów walut zakończone powodzeniem.")
            return aktualne_kursy_walut


        def zbadaj_dane_inflacyjne(self):

            """

            Funkcja zwraca trzy rezultaty swojego działania:
            - query_job_1.to_dataframe() - DataFrame z danymi inflacyjnymi.
            - query_job_2.to_dataframe() - DataFrame z danymi wszystkich tranakcji na obligacjach skarbowych.
            - query_job_3.to_dataframe() - DataFrame z danymi marży zarobku dla obligacji skarbowych, na których
                następowały transakcje.
            
            """

            destination_table_1 = f"`{project_id}.{dataset_inflation}.{table_inflation}`"
            destination_table_2 = f"`{project_id}.{dataset_transactions}.{view_transactions}`"
            destination_table_3 = f"`{project_id}.{dataset_instruments}.{table_treasury_bonds}`"

            query_1 = f"""
            SELECT
                inflation,
                date
            FROM {destination_table_1}
            WHERE TRUE
            """

            query_2 = f"""
            SELECT
                Ticker,
                MAX(Transaction_date)                                            AS Transaction_date,   
                SUM(Transaction_amount) - MAX(cumulative_sell_amount_per_ticker) AS Transaction_amount
            FROM {destination_table_2} 
            WHERE TRUE
                AND instrument_type_id = 5        --> Obligacje skarbowe
                AND Transaction_type   <> "Sell"  --> Tylko transakcje zakupowe
            GROUP BY Ticker
            WINDOW
                Ticker_last_amount AS (
                PARTITION BY
                    Ticker
                ORDER BY
                    Transaction_date DESC
                )
            """
            
            query_3 = f"""
            SELECT
                *
            FROM  {destination_table_3}
            WHERE
                TRUE
            """
        
            client = bigquery.Client()
            query_job_1       = client.query(query_1)
            query_job_2       = client.query(query_2)
            query_job_3       = client.query(query_3)
            dane_inflacyjne   = query_job_1.to_dataframe()
            dane_transakcyjne = query_job_2.to_dataframe()
            dane_marz         = query_job_3.to_dataframe()

            return dane_inflacyjne, dane_transakcyjne, dane_marz

        def obligacje_skarbowe(self,
                            dane_inflacyjne, 
                            dane_transakcyjne, 
                            dane_marz):
            
            """
            
            Funkcja wyznacza aktualną wartość obligacji skarbowych znajdujących się w portfelu na podstawie danych transakcyjnych,
                danych inflacji oraz danych marży.
            
            """
            
            dane_inflacyjne.columns = ['Inflacja', 'Początek miesiąca']
            dane_do_analizy = dane_transakcyjne.merge(right=dane_marz, 
                                            how='inner', 
                                            on = 'Ticker')
            result_df = pd.DataFrame(columns=['Ticker', 'Date', 'Current Value'])
            for dane in dane_do_analizy.iterrows():
                ticker             = dane[1].iloc[0]
                data_zakupu        = dane[1].iloc[1]
                wolumen            = dane[1].iloc[2]
                marza_pierwszy_rok = dane[1].iloc[3]
                marza_kolejne_lata = dane[1].iloc[4]
                
                wolumen_jednostkowy = 100
                
                start_value        = wolumen * wolumen_jednostkowy
            
                current_date       = date.today()
                liczba_dni         = (current_date - data_zakupu).days
                liczba_lat         = int(math.modf(liczba_dni/365)[1])
            
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
                data_to_export = result_df.merge(right=dane_do_analizy, 
                                how='inner',
                                left_on=['Ticker', 'Date'],
                                right_on= ['Ticker', 'Transaction_date'])
        
                data_to_export['Date'] = current_date
                data_to_export['Close'] = (data_to_export['Current Value']/data_to_export['Transaction_amount'])

                data_to_export_obligacje = data_to_export.groupby(['Ticker', 'Date']).\
                    apply(lambda x: np.average(x['Close'], \
                    weights=x['Transaction_amount']))\
                    .reset_index(name='Close').\
                    round(decimals = 3)
                data_to_export_obligacje['Volume'] = 0
                data_to_export_obligacje['Turnover'] = 0
                
            return data_to_export_obligacje

        def webscraping_markets_ft_webscraping(self,
                                            present_instruments_ETF,
                                            present_currencies):
            
            """
            Funkcja wyznacza wartość giełdową zagranicznych ETF.
            """
            
            print("Dokonuję webscrapingu dla wybranych instrumentów ETF.")
            result_df = pd.DataFrame()
            current_date = date.today()
            for instrument in present_instruments_ETF.iterrows():

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
            data_to_export = present_instruments_ETF.merge(result_df,
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

            data_to_export_ETFs = data_to_export.loc[:,['Ticker', 
                                                'Date', 
                                                'Close', 
                                                'Turnover', 
                                                'Volume']]
            

            return data_to_export_ETFs
        
        def webscraping_biznesradar(self,
                                    website,
                                    present_instruments_akcje
                                    ):
            
            """
            Funkcja wyznacza aktualną wartość instrumentów finansowych z portalu biznesradar.
            """

            print("Pobieranie danych z portalu biznesradar.")
            with requests.get(website) as r1:
                try:
                    soup1 = BeautifulSoup(r1.text, 'html.parser')
                    trs = soup1.find_all('tr')
                    trs_classes = [tr.get('class') for tr in trs]
                    trs_classes = [" ".join(tr_class) for tr_class in trs_classes if tr_class != None]

                    if 'ad' in trs_classes:
                        trs_classes.remove('ad')
                    current_date = date.today()
                    result_df = pd.DataFrame()

                    list_of_present_tickers = list(present_instruments_akcje['Ticker'])

                    dict_of_tickers = {}
                    for index, tr_class in enumerate(trs_classes):
                        Ticker = soup1.find('tr', class_=tr_class).find('a').get_text().split(' ')[0]
                        dict_of_tickers.update({tr_class: Ticker})

                    list_of_trs_to_update = []
                    for key, value in dict_of_tickers.items():
                        if value in list_of_present_tickers:
                            list_of_trs_to_update.append(key)

                    for index, tr_class in enumerate(list_of_trs_to_update):
                        Ticker = soup1.find('tr', class_=tr_class).find('a').get_text().split(' ')[0]
                        Close = soup1.find('tr', class_=tr_class).find('span', class_="q_ch_act").get_text(strip=True).replace(
                            " ", "")
                        if Close:
                            Close = float(Close)
                        else:
                            continue
                        Volume = soup1.find('tr', class_=tr_class).find('span', class_="q_ch_vol").get_text(strip=True).replace(
                            " ", "")
                        if Volume:
                            Volume = int(Volume)
                        else:
                            continue
                        Turnover = soup1.find('tr', class_=tr_class).find('span', class_="q_ch_mc").get_text(
                            strip=True).replace(" ", "")
                        if Turnover:
                            Turnover = int(Turnover)
                        else:
                            continue
                        instruments = [Ticker, current_date, Close, Volume, Turnover]
                        result_df = pd.concat([result_df, pd.DataFrame([instruments])], axis=0)

                    result_df.columns = ['Ticker', 'Date', 'Close', 'Volume', 'Turnover']
                    print("Pobieranie danych z biznesradar zakończone powodzeniem.")

                    return result_df

                except requests.exceptions.Timeout:
                    print("Nie udało się podłączyć do strony biznesradar.pl - timeout.")
                else:
                    print("Nie udało się podłączyć do strony biznesradar.pl - inny błąd.")

        def run_scraper(self):


            """
            Funkcja uruchamiająca sekwencyjne wszystkie poszczególne części składowe:
            - W pierwszym kroku pobierane są do zmiennych aktualne zagraniczne ETF oraz akcji polskich i polskie ETF.
            - W drugim kroku pobierane są do zmiennych dane inflacyjnej, transakcyjne oraz marż związane z transakcjami na obligacjach skarbowych.
            - W trzecim kroku dokonywany jest webscraping dla zagranicznych ETF.
            - W czwartym kroku dokonywany jest webscraping dla akcji polskich oraz ETF.
            - W piątym kroku dokonywane jest obliczenie aktualnej wartości obligacji skarbowych.
            - W szóstym kroku następuje eksport danych akcji polskich, ETF polskich, ETF zagranicznych oraz obligacji skarbowych do tabeli w BigQuery danymi giełdowymi.
            - W siódmym kroku dokonywany jest eksport danych walutowych do tabeli w BigQuery.
            """

            present_instruments_ETF, present_instruments_akcje = self.pobierz_aktualne_instrumenty()
            dane_inflacyjne, dane_transakcyjne, dane_marz = self.zbadaj_dane_inflacyjne()
            present_currencies = self.znajdz_kursy_walut()
            data_to_export_ETFs = self.webscraping_markets_ft_webscraping(present_instruments_ETF,
                                                                        present_currencies)
            data_to_export_akcje = self.webscraping_biznesradar(website_stocks, present_instruments_akcje)
            data_to_export_etfs_pl = self.webscraping_biznesradar(website_etfs_pl, present_instruments_akcje)
            data_to_export_obligacje = self.obligacje_skarbowe(dane_inflacyjne,
                                                            dane_transakcyjne,
                                                            dane_marz)

            data_to_export = pd.concat([data_to_export_ETFs, 
                                        data_to_export_akcje, 
                                        data_to_export_etfs_pl,
                                        data_to_export_obligacje],
                                        ignore_index = True)
            
            exporterObject = BigQueryExporter(project_to_export=project_id,
                                                dataset_to_export_daily=dataset_instruments,
                                                dataset_to_export_currencies= dataset_currencies,
                                                table_to_export_daily=table_daily,
                                                table_to_export_currencies=table_currencies
                                                )
            
            exporterObject.exportDataToBigQueryDailyTable(data_to_export = data_to_export)

            exporterObject.exportDatatoBigQueryCurrencyTable(currencies_to_export = present_currencies)


    class BigQueryExporter():

        def __init__(self,
                        project_to_export,
                        dataset_to_export_daily,
                        dataset_to_export_currencies,
                        table_to_export_daily,
                        table_to_export_currencies):
            
            """
            
            Utworzenie obiektu, który umożliwi późniejszy eksport danych do tabel w BigQuery.
            
            """
            
            self.project_to_export = project_to_export
            self.dataset_to_export_daily = dataset_to_export_daily
            self.dataset_to_export_currencies = dataset_to_export_currencies
            self.table_to_export_daily = table_to_export_daily
            self.table_to_export_currencies = table_to_export_currencies
        
        def exportDataToBigQueryDailyTable(self,
                                data_to_export):
            

            """
            
            Eksport danych do tabeli `Daily`.
            
            """
            
            print("Eksportuję dane giełdowe do BigQuery..")
            client = bigquery.Client()
            destination_table = f"{self.project_to_export}.{self.dataset_to_export_daily}.{self.table_to_export_daily}"
            
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
                print("Dane giełdowe zostały wyeksportowane do tabeli BigQuery.")
            except Exception as e:
                print(f"Error uploading data to BigQuery: {str(e)}")

        def exportDatatoBigQueryCurrencyTable(self,
                                                currencies_to_export):
            

            """
            Eksport danych do tabeli Currency.
            """
            
            print("Eksportuję dane walutowe do BigQuery..")
            client = bigquery.Client()
            destination_table = f"{self.project_to_export}.{self.dataset_to_export_currencies}.{self.table_to_export_currencies}"
            
            schema = [bigquery.SchemaField(name = 'Currency_date', field_type = "DATE", \
                                        mode = "NULLABLE"),
                    bigquery.SchemaField(name = 'Currency', field_type = "STRING",\
                                        mode = "NULLABLE"),
                    bigquery.SchemaField(name = 'Currency_close', field_type = "FLOAT",\
                                        mode = "NULLABLE"),
                                        ]
            
            job_config = bigquery.LoadJobConfig(schema = schema,
                                                write_disposition = "WRITE_APPEND")

            try:
                job = client.load_table_from_dataframe(currencies_to_export,
                                                        destination_table,
                                                        job_config = job_config)
                job.result()
                print("Dane walutowe zostały wyeksportowane do tabeli BigQuery.")
            except Exception as e:
                print(f"Error uploading data to BigQuery: {str(e)}")
        

    project_id              = 'projekt-inwestycyjny'
    dataset_instruments     = 'Dane_instrumentow'
    dataset_currencies      = 'Waluty'
    dataset_daily           = 'Dane_instrumentow'
    dataset_inflation       = 'Inflation'
    dataset_transactions    = 'Transactions'
    table_instruments       = 'Instruments'
    table_instruments_types = 'Instrument_types'
    table_currencies        = 'Currency'
    table_daily             = 'Daily'
    table_inflation         = 'Inflation'
    table_treasury_bonds    = 'Treasury_Bonds'
    view_transactions       = 'Transactions_view'
    website_stocks          = 'https://www.biznesradar.pl/gielda/akcje_gpw'
    website_etfs_pl         = 'https://www.biznesradar.pl/gielda/etf'

    scraper = Scraper(project_id, 
                    dataset_instruments,
                    dataset_currencies,
                    dataset_daily,
                    dataset_inflation,
                    dataset_transactions,
                    table_instruments, 
                    table_instruments_types,
                    table_currencies,
                    table_daily,
                    table_inflation,
                    table_treasury_bonds,
                    view_transactions,
                    website_stocks,
                    website_etfs_pl
                    )

    scraper.run_scraper()


""""
Konfiguracja:
    Region: europe-central2 
    Typ aktywatora: Pub/Sub
    Pamięć przydzielona: 512 MiB
    CPU: 0.333
    Przekroczony limit czasu: 60
    Maksymalna liczba żądań na instancję: 1
    Minimalna liczba instancji: 0
    Maksymalna liczba instancji: 1
    Konto usługi srodowiska wykonawczego:
        compute@developer.gserviceaccount.com 
    
Punkt wejscia:
    daily_webscraping_plus_currencies

Requirements:
    functions-framework==3.*
    bs4
    requests
    datetime
    numpy
    pandas
    pandas_gbq
    google.cloud
"""