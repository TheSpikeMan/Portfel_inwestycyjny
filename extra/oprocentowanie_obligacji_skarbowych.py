import base64
import functions_framework
import pandas as pd
import pandas_gbq
import requests
from bs4 import BeautifulSoup
from google.cloud import bigquery
import re

@functions_framework.cloud_event
def Treasury_bonds(cloud_event):
    # 1. Zdefiniowanie parametrów tabeli, przechowującej danej transakcyjne.
    project_id = 'projekt-inwestycyjny'
    dataset_id = 'Transactions'
    table_id = 'Transactions_view'
    destination_table = f"`{project_id}.{dataset_id}.{table_id}`"

    # 1.5. Zdefiniowanie parametrów tabeli, do której pisze CF.
    project_id_2 = 'projekt-inwestycyjny'
    dataset_id_2 = 'Dane_instrumentow'
    table_id_2 = 'Treasury_Bonds'
    destination_table_2 = f"{project_id_2}.{dataset_id_2}.{table_id_2}"

    # 2. Zdefiniowanie kwerendy wyciągającej wyłącznie dane transakcyjne obligacji.
    query = f"""
        SELECT
            *
        FROM  {destination_table}
        WHERE
            Instrument_type_id = 5
            """

    # 3. Utworzenie obiektu Client w ramach biblioteki bigquery, a następnie
    # wyodrębnienie z obiektu QueryJob obiektu do DataFrame.
    client = bigquery.Client()
    query_job = client.query(query)
    dane_transakcyjne = query_job.to_dataframe()

    # 4. Zestawienie wszystkich instrumentów obligacji, na których dokonywane były
    # transakcje.
    ticker_list = list(set(dane_transakcyjne['Ticker'].to_list()))
    tablica_oprocentowania = pd.DataFrame()

    # 5. Dla każdego instrumentu obligacji skarbowych.
    for ticker in ticker_list:

        # 6. Jeżeli analizujemy obligacje EDO
        if ticker.startswith("EDO"):
            website = "https://www.obligacjeskarbowe.pl/oferta-obligacji/obligacje-10-letnie-edo/" +\
                ticker + "/"
        elif ticker.startswith("TOS"):
            website = "https://www.obligacjeskarbowe.pl/oferta-obligacji/obligacje-3-letnie-tos/" + \
                      ticker + "/"

        with requests.get(website) as r:
        # 7. Jeżeli udało się podłączyć do strony.
            if r.status_code == 200:
                raw_data = r.text
                soup = BeautifulSoup(raw_data, 'html.parser')

                # 8. Schemat to obiekt, który zczyna się od znaku dziesiętnego,
                # następnie zawiera przecinek, dwa znaki dziesiętne, dokładnie
                # jeden znak '%', a następnie dowolną liczba dowolnych znaków.
                pattern = re.compile(r'\d,\d{2}%{1}.*')

                # 9. Przeszukiwane konkretnego elementu strony.
                result_set = soup.find_all('span', class_='product-details__list-value')
                raw_string = result_set[1].text

                # 10. Rozdzielenie całego tekstu do pojedynczych słów/
                string_modified = raw_string.split()
                for word in string_modified:
                    # 11. Jeżeli znajdziesz słowo, które spełnia pattern
                    if re.search(pattern, word):
                        oprocentowanie_zmienne = (word[:4].replace(",", "."))
                        oprocentowanie_zmienne = float(oprocentowanie_zmienne)

                # 12. Szukanie oprocentowania obligacji w pierwszym roku.
                # Wykorzystanie w tym celu obiektu o zadanych niżej parametrach.
                result_set_2= soup.find_all('figure', class_ = 'hero__image')
                oprocentowanie_pierwszy_rok = result_set_2[0].text.strip()[:4].\
                    replace(",", ".")
                oprocentowanie_pierwszy_rok = float(oprocentowanie_pierwszy_rok)

                # 13. Przypisanie wszystkich elementów do listy.
                tablica_oprocentowania_tickera = [ticker,
                oprocentowanie_pierwszy_rok,
                oprocentowanie_zmienne]
            else:
                pass

        # 14. Połączenie obecnych danych w DataFrame (pierwszy element) z
        # dokładanymi elementami (pd.DataFrame(tablica_(...)))
        tablica_oprocentowania = pd.concat([tablica_oprocentowania,
                                            pd.DataFrame([tablica_oprocentowania_tickera])],
                                           axis = 0)

    # 15. Zmiana nazewnictwa kolumn.
    tablica_oprocentowania.columns = ['Ticker', 'First_year_interest', 'Regular_interest']
    tablica_oprocentowania.reset_index(drop=True, inplace=True)


    # 17. Zdefiniowanie schematu danych w BQ
    schema = [bigquery.SchemaField(name = 'Ticker', field_type = "STRING", \
                                   mode = "REQUIRED"),
              bigquery.SchemaField(name = 'First_year_interest', field_type = "FLOAT",\
                                   mode = "REQUIRED"),
              bigquery.SchemaField(name = 'Regular_interest', field_type = "FLOAT",\
                                   mode = "REQUIRED")]


    # 18. Wyznaczenie konfiguracji dla joba i wykonanie joba.
    job_config = bigquery.LoadJobConfig(schema = schema,
                                        write_disposition = "WRITE_TRUNCATE")
    try:
        job = client.load_table_from_dataframe(tablica_oprocentowania,
                                               destination_table_2,
                                               job_config = job_config)
        job.result()
    except Exception as e:
        print(f"Error uploading data to BigQuery: {str(e)}")
        return "Błąd eksportu danych do BigQuery."

    print("Dane obligacji skarbowych zostały przekazane do tabeli BigQuery.")
    return "Program zakończył się pomyślnie."
    
"""
Konfiguracja:
    
Nazwa funkcji: Treasury_bonds
Region: europe-central2 (Warszawa)
Pamięć przydzielona: 512 MiB
CPU: 0.333
Przekroczony limit czasu: 60
Maksymalna liczba żądań równoczesnych na instancję: 1
Minimalna liczba instancji: 0 
Maksymalna liczba instancji: 1
Konto usługi: Default compute service account 
    
Aktywator Eventarc:
    - Typ aktywatora: Źródła Google
    - Dostawca zdarzeń: Cloud Pub/Sub
    - Zdarzenie: google.cloud.pubsub.topic.v1.messagePublished
    - Region: europe-central2 (Warszawa)
    - Konto usługi: compute@developer.gserviceaccount.com 
    
Requirements:
    
functions-framework==3.*
bs4
requests
pandas
google.cloud
pandas_gbq
    
"""