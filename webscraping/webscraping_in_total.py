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
from typing import Dict, List
from google.api_core.exceptions import GoogleAPICallError

@functions_framework.cloud_event
def daily_webscraping_plus_currencies(cloud_event):

    # --- Stałe ---
    # Stałe wykorzystywane w wyznaczaniu wartości obligacji
    NOMINAL_VALUE = 100
    DAYS_IN_YEAR = 365
    INFLATION_LAG_DAYS = 60

    class BigQueryExporter():

        def __init__(self):
            """
            Utworzenie obiektu, który umożliwi późniejszy eksport danych do tabel w BigQuery.
            """
            self.client                       = bigquery.Client()
        
        def export_dataframes_to_bigquery(self,
                                            data_to_export: Dict[str, pd.DataFrame]):
        
            """
            Eksport danych z DataFrame'ów do wskazanych tabel w BigQuery

            Args:
                data_to_export: Słownik, gdzie kluczem jest pełna nazwa docelowej tabeli BigQuery,
                    a wartością obiekt DataFrame do załadowania.
            """
            
            print("Rozpoczynam eksport danych do BigQuery.")
            job_config = bigquery.LoadJobConfig(
                autodetect=True,
                write_disposition="WRITE_APPEND"
            )

            for destination_table, dataframe in data_to_export.items():
                if dataframe.empty:
                    print(f"INFO: DataFrame dla tabeli '{destination_table}' jest pusty. Pomijam.")
                    continue

                print(f"Eksportuję dane do tabeli '{destination_table}'...")
                try:
                    job = self.client.load_table_from_dataframe(
                        dataframe=dataframe, 
                        destination=destination_table, 
                        job_config=job_config)
                    job.result()
                    table = self.client.get_table(destination_table)
                    print(
                        f"Pomyślnie wyeksportowano {len(dataframe)} pozycji.\n"
                        f"Tabela '{destination_table}' ma teraz {table.num_rows} wierszy."
                        )
                except GoogleAPICallError as e:
                    print(f"Błąd podczas eksportu do '{destination_table}' : {e}.")
                except Exception as e:
                    print(f"Wystąpił nieoczekiwany błąd podczas eksportu do tabeli '{destination_table}': {e}.")

    class Scraper():
        # --- Funkcje pomocniczne związane z obligacjami ---
        @staticmethod
        def _get_interest_rate_for_period(
            purchase_date: date,
            completed_years: int,
            inflation_dict: Dict[str, float],
            ticker: str,
            regular_margin: float) -> float:
            """
            Wyznacza stopę procentową za dany okres

            Returns:
                float: Oprocentowanie w danym roku (uwzględniające inflację i marżę).
            """

            # Określenie daty do sprawdzenia inflacji (2 miesiące przed)
            inflation_check_offset = timedelta(days=(DAYS_IN_YEAR * completed_years) - INFLATION_LAG_DAYS)
            inflation_check_date = purchase_date + inflation_check_offset

            # Dokonuję konwersji daty inflacji na STRING
            inflation_date_key = inflation_check_date.strftime('%Y-%m-01')

            # Wyznaczam wartość inflacji, na podstawie klucza
            inflation_rate = inflation_dict.get(inflation_date_key, 0.0) / 100.0

            # Dla obligacji TOS inflacja jest ignorowana
            interest_rate = (inflation_rate if ticker.startswith("EDO") else 0) + regular_margin
            return interest_rate

        
        @staticmethod
        def _calculate_single_bond_value(row: pd.Series, inflation_dict: Dict[str, float]) -> float:
            """
            Oblicza wartość bieżącą dla pojedynczej tranakcji obligacji.
            """

            # --- Pobranie danych z wiersza ---
            ticker = row['Ticker']
            purchase_date = row['Transaction_date']
            volume = row['Transaction_amount']
            first_year_margin = row['First_year_interest'] / 100.0
            regular_margin = row['Regular_interest'] / 100.0

            # --- Obliczenia bazowe ---
            start_value = volume * NOMINAL_VALUE
            days_held = (date.today() - purchase_date).days

            # Przypadek 1: Obligacja trzymana krócej niż rok:
            if days_held < DAYS_IN_YEAR:
                # Odsetki naliczane proporcjonalnie
                return start_value * (1 + (days_held / DAYS_IN_YEAR) * first_year_margin)
            
            # --- Obliczenia dla obligacji trzymanych dłużej niż rok

            # Wartość po pierwszym pełnym roku
            current_value = start_value * (1 + first_year_margin)
            remaining_days = days_held - DAYS_IN_YEAR
            completed_years = 1

            # Pętla po kolejnych zakończonych latach
            while remaining_days >= DAYS_IN_YEAR:

                interest_rate = Scraper._get_interest_rate_for_period(purchase_date, completed_years, inflation_dict, ticker, regular_margin)
                # Kapitalizacja odsetek na koniec roku
                current_value *= (1 + interest_rate)

                remaining_days -= DAYS_IN_YEAR
                completed_years += 1
            
            # Doliczenie odsetek za bieżący, niepełny rok
            if remaining_days > 0:
                # Określenie daty do sprawdzenia inflacji (2 miesiące przed)
                interest_rate = Scraper._get_interest_rate_for_period(purchase_date, completed_years, inflation_dict, ticker, regular_margin)

                # Proporcjonalne odsetki za dni w bieżącym okrese
                current_value *= (1 + (remaining_days / DAYS_IN_YEAR) * interest_rate)
            
            return current_value
        
        # --- Inicjalizacja obiektu ---
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
            self.client                  = bigquery.Client()

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

            try:
                query_job_1 = self.client.query(query=query_1)
                query_job_2 = self.client.query(query=query_2)
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
        
            query_job_1       = self.client.query(query_1)
            query_job_2       = self.client.query(query_2)
            query_job_3       = self.client.query(query_3)
            dane_inflacyjne   = query_job_1.to_dataframe()
            dane_transakcyjne = query_job_2.to_dataframe()
            dane_marz         = query_job_3.to_dataframe()

            print("Pobieranie aktualnych danych inflacyjnch zakończone powodzeniem.")

            return dane_inflacyjne, dane_transakcyjne, dane_marz
        
            
        def treasury_bonds(
                self,
                dane_inflacyjne: pd.DataFrame, 
                dane_transakcyjne: pd.DataFrame, 
                dane_marz: pd.DataFrame):
        
            """
            Funkcja wyznacza aktualną wartość obligacji skarbowych znajdujących się w portfelu.

            Obsługiwane obligacje: EDO (indeksowane inflacją), TOS (stałoprocentowe)
            """

            print("Rozpoczynam ocenę wartości obligacji skarbowych.")

            # --- Definicja obsługiwanych typów obligacji skarbowych ---
            SUPPORTED_BONDS = ['EDO', 'TOS']
            
            # --- Przygotowanie danych ---
            inflation_data = dane_inflacyjne.copy()
            inflation_data.columns = ['Inflacja', 'Początek miesiąca']
            inflation_data['Początek miesiąca'] = pd.to_datetime(inflation_data['Początek miesiąca']).dt.strftime('%Y-%m-01')

            # Tworzymy słownik inflacji
            inflation_dict = dict(zip(inflation_data['Początek miesiąca'], inflation_data['Inflacja']))

            # Łączę dane transakcyjne z danymi marż
            analysis_data = dane_transakcyjne.merge(
                right=dane_marz,
                how='inner',
                on='Ticker'
            ).copy()

            # Filtrujemy tylko obsługiwane obligacje
            analysis_data = analysis_data[analysis_data['Ticker'].str.startswith(tuple(SUPPORTED_BONDS))].copy()

            # Obsługa potencjalnie pustej ramki danych
            if analysis_data.empty:
                print("Nie znaleziono obsługiwanych obligacji do analizy.")
                return pd.DataFrame()

            # --- Główna logika obliczeniowa ---
            analysis_data['Current_Value'] = analysis_data.apply(
                lambda row: Scraper._calculate_single_bond_value(row, inflation_dict),
                axis=1
            )

            # --- Agregacja wyników ---

            analysis_data['Close'] = analysis_data['Current_Value'] / analysis_data['Transaction_amount']
            analysis_data['weighted_close'] = analysis_data['Close'] * analysis_data['Transaction_amount']

            grouped = analysis_data.groupby(['Project_id', 'Ticker'])

            result = (
                grouped['weighted_close'].sum() / grouped['Transaction_amount'].sum()
            ).reset_index(name='Close')

            result['Date'] = date.today()
            result['Close'] = result['Close'].round(2)
            result['Volume'] = 0
            result['Turnover'] = 0

            print("Ocena wartości obligacji skarbowych zakończona powodzeniem.")
            return result
        
        def webscraping_markets_ft_webscraping(self,
                                               present_instruments_ETF,
                                               present_currencies):
            
            """
            Funkcja wyznacza wartość giełdową zagranicznych ETF.
            Args:
                present_instruments_ETF: (pd.DataFrame): DataFrame zawierający dane instrumentów objętych webscrapingiem.
                present_currencies: (pd.DataFrame): DataFrame z kursami walut, dla powyższych instrumentów
            Returns:
                result_df: (pd.DataFrame): DataFrame z przetworzonymi danymi
            """
            
            # --- Zdefiniowanie wszystkich elementów startowych ---
            base_url = "https://markets.ft.com/data/etfs/tearsheet/summary?s="
            scraped_data = []
            current_date = date.today()
            analysis_data = present_instruments_ETF.copy()
            currencies_data = present_currencies.copy()

            # --- Webscraping ---
            print("Dokonuję webscrapingu z 'markets.ft.com'.")
            for row in analysis_data.itertuples():
                url = f"{base_url}{row.ticker}:{row.market}:{row.market_currency}"
                try:
                    with requests.get(url=url, timeout=10) as response:
                        # Obsługa wyjątków
                        response.raise_for_status()                    
                        soup = BeautifulSoup(response.text, 'html.parser')
                        price_span = soup.find('span', class_ = 'mod-ui-data-list__value')
                        if not price_span:
                            print(f"Ostrzeżenie: Nie znaleziono ceny dla {row.ticker}. Pomijam.")
                        close_price = float(price_span.text.replace(',', ''))
                        # Dodajemys łownik do listy
                        scraped_data.append({
                                            "Ticker": row.ticker,
                                            "Close_foreign": close_price,
                                            "Currency": row.market_currency})
                except requests.RequestException as e:
                    print(f"Błąd podczas pobierania danych dla {row.ticker}: {e}")
                except (ValueError, AttributeError) as e:
                    print(f"Błąd podczas parsowania danych dla {row.ticker}: {e}")

            result_df = pd.DataFrame(data=scraped_data)
            final_df= pd.merge(left=result_df, 
                                 right=currencies_data,
                                 how='inner',
                                 on='Currency')
            # Obliczenia na połączonym DataFrame
            final_df['Close'] = (final_df['Close_foreign'] * final_df['Currency_close']).round(2)
            final_df['Date'] = current_date
            final_df['Project_id'] = np.nan
            final_df['Turnover'] = 0
            final_df['Volume'] = 0

            output_columns = ['Project_id', 'Ticker', 'Date', 'Close', 'Turnover', 'Volume']
            final_df = final_df[output_columns]
            print("Webscraping z 'markets.ft.com' zakończony powodzeniem.")
            return final_df
        
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
            dane_inflacyjne, dane_transakcyjne, dane_marz = self.zbadaj_dane_inflacyjne()
            present_currencies = self.znajdz_kursy_walut()
            data_to_export_akcje = self.webscraping_biznesradar(website_stocks, present_instruments_biznesradar)
            data_to_export_ETFs = self.webscraping_markets_ft_webscraping(present_instruments_ETF,present_currencies)
            #data_to_export_catalyst = self.webscraping_biznesradar(website_catalyst, present_instruments_biznesradar) DO POPRAWKI
            data_to_export_obligacje = self.treasury_bonds(dane_inflacyjne,dane_transakcyjne,dane_marz)
            data_to_export_etfs_pl = self.webscraping_biznesradar(website_etfs_pl, present_instruments_biznesradar)
            
            data_to_export = pd.concat([data_to_export_ETFs, 
                                        data_to_export_akcje,
                                        #data_to_export_catalyst, 
                                        data_to_export_etfs_pl,
                                        data_to_export_obligacje],
                                        ignore_index = True)
            # Eksport danych
            exporterObject = BigQueryExporter()
            destination_table_daily = f"{self.project_id}.{self.dataset_daily }.{self.table_daily}"
            destination_table_currencies = f"{self.project_id}.{self.dataset_currencies}.{self.table_currencies}"
            exporterObject.export_dataframes_to_bigquery(data_to_export={destination_table_currencies:present_currencies,
                                                                            destination_table_daily:data_to_export})

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