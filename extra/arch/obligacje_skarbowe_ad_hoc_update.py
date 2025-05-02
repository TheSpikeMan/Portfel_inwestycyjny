import pandas as pd
from datetime import date, datetime, timedelta
import math
from google.cloud import bigquery
import numpy as np

"""
Kod służy do dogrania danych obligacji skarbowych do okresu, który można
zdefiniować w zmiennej data_maksymalna.

Dobiera on na podstawie innych tabel w BigQuery wszystkie niezbędne dane
i uzupełnia dane obligacji skarbowych od daty zakupu danego instrumentu do
daty okrelosnej w zmiennej data_maksymalna.
"""

data_maksymalna = '2024-01-15'
data_maksymalna = datetime.strptime(data_maksymalna, "%Y-%m-%d").date()

# 1. Zdefiniowanie charakterystyk baz danych, wykorzystywanych w programie.

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

# 2. Zdefiniowanie zapytań do baz danych, wykorzystywanych w programie.

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

# 3. Utworzenie obiektów QueryJob, a następnie odczyt do obiektów DataFrame.

client = bigquery.Client()
query_job_1 = client.query(query_1)
query_job_2 = client.query(query_2)
query_job_3 = client.query(query_3)
dane_inflacyjne = query_job_1.to_dataframe()
dane_transakcyjne = query_job_2.to_dataframe()
dane_marz = query_job_3.to_dataframe()

# 4. Zmiana nazewnictwa kolumn w danych inflacyjnych.

dane_inflacyjne.columns = ['Inflacja', 'Początek miesiąca']

# 5. Połączenie danych transakcyjnych z danymi marż.

dane_obligacji = dane_transakcyjne.merge(right=dane_marz,
                                         how='inner',
                                         on='Ticker')

# 6. Wyciągnięcie tylko okreslonych kolumn.

dane_do_analizy = dane_obligacji.loc[:,['Ticker', 'Transaction_date',
                                        'Transaction_amount',\
                                        'First_year_interest',\
                                        'Regular_interest']]

# 7. Stworzenie DataFrame do przechowywania danych.
result_df = pd.DataFrame(columns=['Ticker', 'Date', 'Current Value'])

# 8. Dla każdego wiersza z danych transakcyjnych wykonaj:
for dane in dane_do_analizy.iterrows():
    
    # 9. Przypisz wartosc parametrów wg danych transakcyjnych.
    ticker = dane[1].iloc[0]
    data_zakupu = dane[1].iloc[1]
    wolumen = dane[1].iloc[2]
    marza_pierwszy_rok = dane[1].iloc[3]
    marza_kolejne_lata = dane[1].iloc[4]

    # 10. Wyznaczenie wartosci poczatkowej danej liczby obligacji.
    # Wolumen_jednostkowy oznacza pojedyncza wartosc jednej obligacji.
    wolumen_jednostkowy = 100

    # 11. Wyznaczenie początkowej wartosci danego pakietu obligacji,
    # o okreslonym tickerze.
    start_value = wolumen * wolumen_jednostkowy
    
    # 12. Zdefiniowanie zmiennej 'step' przechowującej wartosc kroku
    # o który zmieniana będzie data w poniższej pętli while.
    step = timedelta(days=1)
    
    # 13. Przypisanie do zmiennej 'current_date' pierwszej daty, dla której
    # analizowana jest wartosc. Zwykle jest to jeden dzień plus od
    # daty zakupu.
    current_date = data_zakupu + step

    
    # 14. Realizacja pętli while w zakresie od daty - dzień po zakupie
    # do daty maksymalnej okreslonej przez użytkownika
    
    while current_date <= data_maksymalna:
        # 15. Wyznaczenie liczby dni posiadania danej obligacji.
        liczba_dni = (current_date - data_zakupu).days
        print("Liczba dni wynosi ", liczba_dni)
        
        # 16. Wyznaczenie wartosci bezwzględnej liczby lat posiadania danej obligacji.
        liczba_lat = int(math.modf(liczba_dni/365)[1])

        # 17. Wyznaczenie danego parametru n.
        n = 1

        # 18. Jeżeli liczba dni jest mniejsza niż 365 zastosuj poniższą formułę
        # do wyznaczenie aktualnej wartosci.
        if liczba_dni < 365:
            current_value = start_value + start_value * liczba_dni / 365 * (marza_pierwszy_rok/100)

        # 19. Jeżeli liczba dni jest większa lub równa 365 dni dokonuj analizy wg
        # poniższego kodu.
        else:
            current_value = start_value + start_value * (marza_pierwszy_rok/100)

            # 20. Pętla wykonywana jest tyle razy, aż liczba dni spadnie poniżej 365.
            for i in range(liczba_lat, 0, -1):

                # 21. Przesunięcie daty badania inflacji występuje o okres równy
                # ilosci lat i dwoch miesiecy.
                liczba_dni_przesuniecie = timedelta(days= 365 * n - 60)

                # 22. Data badania inflacji jest wynikiem różnicy pomiedzy datą
                # transakcji (data_zakupu), a okresem czasowy wynikającym z powyższego
                # obliczenia (liczba_dni_przesuniecie)
                data_badania_inflacji = date((data_zakupu + liczba_dni_przesuniecie).year, \
                                     (data_zakupu + liczba_dni_przesuniecie).month, \
                                     1)
                # 23. Wartosci inflacji jest wyznaczana dla daty_badania_inflacji
                inflacja = dane_inflacyjne.loc[dane_inflacyjne['Początek miesiąca'] \
                                              == str(data_badania_inflacji)].iat[0,0]

                # 24. Jeżeli liczba dni jest mniejesza niz 720 wpada do tej czesci pętli.
                if liczba_dni < 730:
                    current_value = current_value + current_value * \
                        (liczba_dni - 365)/365 * (inflacja + marza_kolejne_lata)/ 100
                else:
                    current_value = current_value + current_value * \
                        (inflacja + marza_kolejne_lata) / 100
                    liczba_dni = liczba_dni - 365
                n = n + 1 

        # 25. Dołączanie danych dla danego tickera z pozostałymi danymi.
        result_df = pd.concat([result_df, \
                               pd.DataFrame(data=[[ticker, data_zakupu, current_date, \
                                                   round(current_value, 2)]], \
                                            columns=['Ticker', 'Date', 'Current date', 'Current Value'])])
        
        # 26. Dołączenie do danych wyliczonych, danych transakcji - dzięki czemu
        # w kolejnym kroku możliwe jest wyznaczenie sredniej ceny
        # w zależnosci od wolumenu
        data_to_export = result_df.merge(right=dane_obligacji, 
                        how='inner',
                        left_on=['Ticker', 'Date'],
                        right_on= ['Ticker', 'Transaction_date'])

        # 27. Utworzenie kolumny sredniej wartosci 1 sztuki obligacji skarbowej
        # na dany dzień.
        data_to_export['Close'] = (data_to_export['Current Value']/\
                                   data_to_export['Transaction_amount']).round(3) 
        
        # 28. Dodanie jednego dnia do bieżącej daty, aż do daty maksymalnej.
        current_date = current_date + step 

# 29. Grupowanie danych - wyznaczanie sredniej ceny zamknięcia instrumentu na 
# podstawie wagi wolumenowej, w przypadku, gdy instrument został
# zakupiony wiecej niż raz.

data_to_export = data_to_export.groupby(['Ticker', 'Current date']).\
    apply(lambda x: np.average(x['Close'], \
    weights=x['Transaction_amount']))\
    .reset_index(name='Close').\
    round(decimals = 3)
data_to_export.rename(columns = {'Current date': 'Date'}, inplace=True)

data_to_export['Volume'] = 0
data_to_export['Turnover'] = 0

# 30. Przygotowanie schematu danych w BQ, wg którego importowane będą dane.
schema = [bigquery.SchemaField(name='Ticker', field_type="STRING",
                               mode="REQUIRED"),
          bigquery.SchemaField(name='Date', field_type="DATE",
                               mode="REQUIRED"),
          bigquery.SchemaField(name='Close', field_type="FLOAT",
                               mode="REQUIRED"),
          bigquery.SchemaField(name='Volume', field_type="INTEGER",
                               mode="REQUIRED"),
          bigquery.SchemaField(name='Turnover', field_type="INTEGER",
                               mode="NULLABLE")]

# 31. Zdefiniowanie parametrów tabeli, do której pisze CF.
project_id = 'projekt-inwestycyjny'
dataset_id = 'Dane_instrumentow'
table_id = 'Daily'
destination_table = f"{project_id}.{dataset_id}.{table_id}"

# 32. Wyznaczenie konfiguracji dla joba i wykonanie joba.
job_config = bigquery.LoadJobConfig(schema=schema,
                                    write_disposition="WRITE_APPEND")

# 33. Wykonanie operacji eksportu danych.
try:
    job = client.load_table_from_dataframe(data_to_export,
                                           destination_table,
                                           job_config=job_config)
    job.result()
except Exception as e:
    print(f"Error uploading data to BigQuery: {str(e)}")

print("Dane obligacji skarbowych zostały przekazane do tabeli BigQuery.")
