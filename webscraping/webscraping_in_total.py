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
from typing import Dict

@functions_framework.cloud_event
def daily_webscraping_plus_currencies(cloud_event):

    # --- Stałe ---
    # Stałe wykorzystywane w wyznaczaniu wartości obligacji
    NOMINAL_VALUE = 100
    DAYS_IN_YEAR = 365
    INFLATION_LAG_DAYS = 60

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
            
            self.project_to_export            = project_to_export
            self.dataset_to_export_daily      = dataset_to_export_daily
            self.dataset_to_export_currencies = dataset_to_export_currencies
            self.table_to_export_daily        = table_to_export_daily
            self.table_to_export_currencies   = table_to_export_currencies
        
        def exportDataToBigQueryDailyTable(self, data_to_export):
            

            """
            
            Eksport danych do tabeli `Daily`.
            
            """
            
            print("Eksportuję dane giełdowe do BigQuery..")
            client = bigquery.Client()
            destination_table = f"{self.project_to_export}.{self.dataset_to_export_daily}.{self.table_to_export_daily}"
            
            schema = [
                    bigquery.SchemaField(name='Project_id', field_type="INTEGER", \
                                        mode="REQUIRED"),
                    bigquery.SchemaField(name = 'Ticker', field_type = "STRING", \
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
                    website_etfs_pl,
                    website_catalyst):
            
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
            self.website_catalyst        = website_catalyst

        def pobierz_aktualne_instrumenty(self):

            """
            Pobranie listy aktualnych instrumentów ETF - query_job_1.to_dataframe()
            Pobranie listy aktualnych instrumentów akcji polskich, ETF polskich, obligacji korporacyjnych - query_job_2.to_dataframe()
            """

            print("Pobieram aktualne instrumenty w ramach ETF zagranicznych.")
            query_1 = f"""
            SELECT DISTINCT
                ticker,
                market,
                market_currency,
                instrument_type
            FROM `{self.project_id}.{self.dataset_instruments}.{self.table_instruments}` AS inst
            INNER JOIN `{self.project_id}.{self.dataset_instruments}.{self.table_instruments_types}` AS inst_typ
                ON inst.Instrument_type_id = inst_typ.Instrument_type_id
                AND Instrument_type = 'ETF akcyjne zagraniczne'
            """

            print("Pobieram aktualne instrumenty w ramach akcji polskich, ETF polskich oraz obligacji korporacyjnych")
            query_2 = f"""
            SELECT DISTINCT
                ticker
            FROM `{self.project_id}.{self.dataset_instruments}.{self.table_instruments}` AS inst
            INNER JOIN `{self.project_id}.{self.dataset_instruments}.{self.table_instruments_types}` AS inst_typ
                ON inst.Instrument_type_id = inst_typ.Instrument_type_id
                AND Instrument_type IN ('Akcje polskie',
                                        'ETF obligacyjne polskie',
                                        'Obligacje korporacyjne')
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
            - query_job_2.to_dataframe() - DataFrame z danymi wszystkich transakcji na obligacjach skarbowych.
            - query_job_3.to_dataframe() - DataFrame z danymi marży zarobku dla obligacji skarbowych, na których
                następowały transakcje.
            
            """

            print("Pobieram aktualne dane inflacyjne.")
            destination_table_1 = f"`{self.project_id}.{self.dataset_inflation}.{self.table_inflation}`"
            destination_table_2 = f"`{self.project_id}.{self.dataset_transactions}.{self.view_transactions}`"
            destination_table_3 = f"`{self.project_id}.{self.dataset_instruments}.{self.table_treasury_bonds}`"

            query_1 = f"""
            SELECT
                inflation,
                date
            FROM {destination_table_1}
            WHERE TRUE
            """

            query_2 = f"""
            WITH treasury_bonds AS (
            SELECT
                Project_id                                                       AS Project_id,
                Ticker                                                           AS Ticker,
                MAX(Transaction_date)                                            AS Transaction_date,   
                CAST(
                    SUM(Transaction_amount) - MAX(cumulative_sell_amount_per_ticker)
                    AS INT64)                                                    AS Transaction_amount
            FROM {destination_table_2} 
            WHERE TRUE
                AND instrument_type_id = 5        --> Obligacje skarbowe
                AND Transaction_type   <> "Sell"  --> Tylko transakcje zakupowe
            GROUP BY ALL
            )

            SELECT 
                Project_id,
                Ticker,
                Transaction_date,
                Transaction_amount
            FROM treasury_bonds
            WHERE TRUE
                AND Transaction_amount <> 0
                AND Transaction_amount IS NOT NULL
            """

            query_3 = f"""
            SELECT *
            FROM  {destination_table_3}
            WHERE TRUE
            """
        
            client = bigquery.Client()
            query_job_1       = client.query(query_1)
            query_job_2       = client.query(query_2)
            query_job_3       = client.query(query_3)
            dane_inflacyjne   = query_job_1.to_dataframe()
            dane_transakcyjne = query_job_2.to_dataframe()
            dane_marz         = query_job_3.to_dataframe()

            print("Pobieranie aktualnych danych inflacyjnch zakończone powodzeniem.")

            return dane_inflacyjne, dane_transakcyjne, dane_marz

        def obligacje_skarbowe(self,
                        dane_inflacyjne, 
                        dane_transakcyjne, 
                        dane_marz):
        
            """
            Funkcja wyznacza aktualną wartość obligacji skarbowych znajdujących się w portfelu.

            Obsługiwane obligacje: EDO (indeksowane inflacją), TOS (stałoprocentowe)
            """

            print("Rozpoczynam ocenę wartości obligacji skarbowych.")
            
            # --- Przygotowanie danych ---
            inflation_data = dane_inflacyjne.copy()
            inflation_data.columns = ['Inflacja', 'Początek miesiąca']
            inflation_data['Początek miesiąca'] = pd.to_datetime(inflation_data['Początek miesiąca'].dt.strftime('%Y-%m-01'))

            # Tworzymy słownik inflacji
            inflacja_dict = dict(zip(dane_inflacyjne['Początek miesiąca'], dane_inflacyjne['Inflacja']))

            # Łączę dane transakcyjne z danymi marż
            analysis_data = dane_transakcyjne.merge(
                right=dane_marz,
                how='inner',
                on = 'Ticker'
            ).copy()

            # Filtrujemy tylko obsługiwane obligacje
            analysis_data = analysis_data[analysis_data['Ticker'].str.startswith(("EDO", "TOS"))].copy()

            # Definiuję listę do zbierania danych
            results = []
            
            # Iteruję po instrumentach obligacji skarbowych w ramach wszystkich projektów
            for row in analysis_data.itertuples():

                # Wyznaczam podstawowe parametry transakcyjne oraz marżowe
                project_id         = row.Project_id
                ticker             = row.Ticker
                data_zakupu        = row.Transaction_date
                wolumen            = row.Transaction_amount
                marza_pierwszy_rok = row.First_year_interest
                marza_kolejne_lata = row.Regular_interest
                
                # Wartość początkowa jednej obligacji
                wolumen_jednostkowy = 100
                
                start_value        = wolumen * wolumen_jednostkowy

                # Wyznaczam wszystkie niezbędne daty do wyznaczenia wartości obligacji lub inflacji (jeśli dotyczy)
                current_date       = date.today()
                liczba_dni         = (current_date - data_zakupu).days
                liczba_lat         = int(math.modf(liczba_dni/365)[1])
                
                n = 1
                if liczba_dni < 365:
                    current_value = start_value + start_value * liczba_dni / 365 * (marza_pierwszy_rok/100)
                else:
                    current_value = start_value + start_value * (marza_pierwszy_rok/100)
                    for i in range(liczba_lat, 0, -1):
                        # Wyznaczam liczbę dni do przesunięcia, aby wyznaczyć dzień badania inflacji
                        liczba_dni_przesuniecie = timedelta(days= 365 * n - 60)
                        # Wyznaczam datę badania inflacji
                        data_badania_inflacji = date(
                            (data_zakupu + liczba_dni_przesuniecie).year,
                            (data_zakupu + liczba_dni_przesuniecie).month,
                            1)
                        # Wyznaczam wartość inflacji
                        inflacja = inflacja_dict.get(str(data_badania_inflacji), 0)

                        # Uwzględniam inflację lub nie w zależności od typu obligacji (uwzględniam dla EDO, dla TOS nie)
                        uwzgl_infl= inflacja if ticker.startswith("EDO") else 0
                        if liczba_dni < 730:
                            current_value = current_value + current_value * \
                            (liczba_dni - 365)/365 * \
                            (uwzgl_infl + marza_kolejne_lata)/ 100
                        else:
                            current_value = current_value + current_value * \
                                (uwzgl_infl + marza_kolejne_lata) / 100
                            liczba_dni -= 365
                        n = n + 1

                # Dodaję dane do zbiorczej tabeli
                results.append([project_id, ticker, data_zakupu, round(current_value, 2), wolumen])
            
            data_to_export = pd.DataFrame(results, columns=['Project_id', 'Ticker', 'Date', 'Current Value', 'Transaction_amount'])
            data_to_export['Date'] = current_date
            data_to_export['Close'] = data_to_export['Current Value'].div(data_to_export['Transaction_amount'],
                                                                            fill_value=pd.NA)

            # Wyznaczam średnią wartość jednej obligacji, ważąc średnią wolumenem transakcyjnym
            data_to_export['weighted_close'] = (
                data_to_export['Close'] * data_to_export['Transaction_amount']
            )
            group = data_to_export.groupby(['Project_id', 'Ticker', 'Date'])
            data_to_export_obligacje = (
                group['weighted_close'].sum().div(group['Transaction_amount'].sum())
                .reset_index(name='Close')
                .round({'Close': 3})
            )
            data_to_export_obligacje['Volume'] = 0
            data_to_export_obligacje['Turnover'] = 0

            print("Ocena wartości obligacji skarbowych zakończona powodzeniem.")
                
            return data_to_export_obligacje
        
        def webscraping_markets_ft_webscraping(self,
                                            present_instruments_ETF,
                                            present_currencies):
            
            """
            Funkcja wyznacza wartość giełdową zagranicznych ETF.
            """
            
            print("Dokonuję webscrapingu z 'markets.ft.com'.")
            result_df = pd.DataFrame()
            current_date = date.today()
            for instrument in present_instruments_ETF.iterrows():

                url = "https://markets.ft.com/data/etfs/tearsheet/summary?s=" + \
                        f"{instrument[1]['ticker']}:" + \
                        f"{instrument[1]['market']}:" + \
                        f"{instrument[1]['market_currency']}"
                        
                with requests.get(url=url, timeout=10) as r:                    
                    soup = BeautifulSoup(r.text, 'html.parser')
                    close = soup.find_all('span', class_ = 'mod-ui-data-list__value')
                    close = float(close[0].text)
                    result_df = pd.concat([result_df, 
                                        pd.DataFrame([[instrument[1]['ticker'], close]])],
                                        axis = 0)
                    
            result_df.columns = ['Ticker', 'Close']
            data_to_export = present_instruments_ETF.merge(result_df,
                                                    how = 'inner',
                                                    left_on = 'ticker',
                                                    right_on = 'Ticker')
            data_to_export = data_to_export.merge(present_currencies,
                                                how = 'inner',
                                                left_on = 'market_currency',
                                                right_on = 'Currency')
            data_to_export['Project_id'] = np.nan
            data_to_export['Close'] = (data_to_export['Close'] * \
                                    data_to_export['Currency_close']).\
                                    round(decimals = 2)
            data_to_export['Date'] = current_date
            data_to_export['Volume'] = 0 
            data_to_export['Turnover'] = 0 

            data_to_export_ETFs = data_to_export.loc[:,['Project_id',
                                                        'Ticker',
                                                        'Date',
                                                        'Close',
                                                        'Turnover',
                                                        'Volume']]
            
            
            print("Webscraping z 'markets.ft.com' zakończony powodzeniem.")

            return data_to_export_ETFs
        
        def webscraping_biznesradar(self,
                                    website,
                                    present_instruments_biznesradar
                                    ):
                
            """
            Funkcja wyznacza aktualną wartość instrumentów finansowych z portalu biznesradar.
            """

            present_instruments = present_instruments_biznesradar['ticker'].tolist()

            current_date = date.today()
            result_data = []  # Lista na dane do późniejszej konwersji do DataFrame

            print("Pobieranie danych z portalu biznesradar.")
            try:
                with requests.get(website, timeout=10) as r1:
                    # Obsługa statusu odpowiedzi HTTP
                    r1.raise_for_status()

                    # Parsowanie HTML
                    soup = BeautifulSoup(r1.text, 'html.parser')

                    for row in soup.find_all('tr', class_='hot-row'):
                        try:
                            Ticker = row.find('a').text.split()[0].strip()
                            if Ticker in present_instruments:
                                Close = row.find('span', {'data-push-type': 'QuoteClose'})
                                Volume = row.find('span', {'data-push-type': 'QuoteVolume'})
                                Turnover = row.find('span', {'data-push-type': 'QuoteMarketCap'})
                                
                                # Upewniamy się, że elementy istnieją przed pobraniem tekstu
                                Close_value = Close.text.strip() if Close else None
                                Volume_value = Volume.text.strip() if Volume else None
                                Turnover_value = Turnover.text.strip() if Turnover else None

                                # Konwersja Close na float, Volume i Turnover na int, obsługa błędów
                                try:
                                    Close_value = float(Close_value.replace(',', '.')) if Close_value else None
                                except ValueError:
                                    Close_value = None  # Jeśli konwersja nie powiedzie się, ustawiamy None
                                
                                try:
                                    Volume_value = int(Volume_value.replace(' ', '').replace(',', '')) if Volume_value else None
                                except ValueError:
                                    Volume_value = None  # Jeśli konwersja nie powiedzie się, ustawiamy None
                                
                                try:
                                    Turnover_value = int(Turnover_value.replace(' ', '').replace(',', '')) if Turnover_value else None
                                except ValueError:
                                    Turnover_value = None  # Jeśli konwersja nie powiedzie się, ustawiamy None

                                # Dodanie danych do listy
                                result_data.append([Ticker, current_date, Close_value, Volume_value, Turnover_value])
                        except AttributeError as e:
                            print(f"Błąd podczas parsowania wiersza: {e}")
                            continue


                    # Przekształcenie wyników na DataFrame
                    result_df = pd.DataFrame(result_data, columns=['Ticker', 'Date', 'Close', 'Volume', 'Turnover'])

                    # Zdefiniowanie nowej kolumny o nazwie 'Project_id i dodanie jej na początku DataFrame
                    result_df.insert(0, 'Project_id', np.nan)
                    print("Pobieranie danych z biznesradar zakończone powodzeniem.")

            except requests.exceptions.Timeout:
                print("Nie udało się podłączyć do strony biznesradar.pl - timeout.")
            except requests.exceptions.RequestException as e:
                print(f"Nie udało się podłączyć do strony biznesradar.pl - błąd HTTP: {e}")
            except Exception as e:
                print(f"Wystąpił nieoczekiwany błąd: {e}")
            
            return result_df

        def run_scraper(self):


            """
            Funkcja uruchamiająca sekwencyjne wszystkie poszczególne części składowe:
            - W pierwszym kroku pobierane są do zmiennych aktualne zagraniczne ETF oraz akcji polskich i polskie ETF.
            - W drugim kroku pobierane są do zmiennych dane inflacyjnej, transakcyjne oraz marż związane z transakcjami na obligacjach skarbowych.
            - W trzecim kroku dokonywany jest webscraping dla zagranicznych ETF.
            - W czwartym kroku dokonywany jest webscraping dla akcji polskich oraz polskich ETF.
            - W piątym kroku dokonywane jest obliczenie aktualnej wartości obligacji skarbowych.
            - W szóstym kroku następuje eksport danych akcji polskich, ETF polskich, ETF zagranicznych oraz obligacji skarbowych do tabeli w BigQuery danymi giełdowymi.
            - W siódmym kroku dokonywany jest eksport danych walutowych do tabeli w BigQuery.
            """

            
            present_instruments_ETF, present_instruments_biznesradar = self.pobierz_aktualne_instrumenty()
            dane_inflacyjne, dane_transakcyjne, dane_marz            = self.zbadaj_dane_inflacyjne()
            present_currencies                                       = self.znajdz_kursy_walut()
            data_to_export_akcje                                     = self.webscraping_biznesradar(website_stocks, 
                                                                                                    present_instruments_biznesradar)
            data_to_export_ETFs                                      = self.webscraping_markets_ft_webscraping(
                                                                                                    present_instruments_ETF,
                                                                                                    present_currencies)
            #data_to_export_catalyst = self.webscraping_biznesradar(website_catalyst, present_instruments_biznesradar) DO POPRAWKI
            
            data_to_export_obligacje                                 = self.obligacje_skarbowe(dane_inflacyjne,
                                                                                                dane_transakcyjne,
                                                                                                dane_marz)
            data_to_export_etfs_pl                                   = self.webscraping_biznesradar(
                                                                                                website_etfs_pl, 
                                                                                                present_instruments_biznesradar)
            
            data_to_export = pd.concat([data_to_export_ETFs, 
                                        data_to_export_akcje,
                                        #data_to_export_catalyst, 
                                        data_to_export_etfs_pl,
                                        data_to_export_obligacje],
                                        ignore_index = True)
                
            exporterObject = BigQueryExporter(project_to_export=self.project_id,
                                                dataset_to_export_daily=self.dataset_instruments,
                                                dataset_to_export_currencies= self.dataset_currencies,
                                                table_to_export_daily=self.table_daily,
                                                table_to_export_currencies=self.table_currencies
                                                )
            
            exporterObject.exportDataToBigQueryDailyTable(data_to_export = data_to_export)

            exporterObject.exportDatatoBigQueryCurrencyTable(currencies_to_export = present_currencies)
            
            
            
    # Definiowanie nazwy projektu
    project_id              = 'projekt-inwestycyjny'

    # Definiowanie nazw zbiorów tabel
    dataset_instruments     = 'Dane_instrumentow'
    dataset_currencies      = 'Waluty'
    dataset_daily           = 'Dane_instrumentow'
    dataset_inflation       = 'Inflation'
    dataset_transactions    = 'Transactions'

    # Definiowanie nazw tabel
    table_instruments       = 'Instruments'
    table_instruments_types = 'Instrument_types'
    table_currencies        = 'Currency'
    table_daily             = 'Daily'
    table_inflation         = 'Inflation'
    table_treasury_bonds    = 'Treasury_Bonds'

    # Definiowanie nazw widoków
    view_transactions       = 'Transactions_view'

    # Definiowanie nazw stron do scrapingu
    website_stocks          = 'https://www.biznesradar.pl/gielda/akcje_gpw'
    website_etfs_pl         = 'https://www.biznesradar.pl/gielda/etf'
    website_catalyst        = 'https://www.biznesradar.pl/gielda/obligacje'

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
                    website_etfs_pl,
                    website_catalyst 
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