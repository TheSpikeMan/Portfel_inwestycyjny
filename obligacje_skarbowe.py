# -*- coding: utf-8 -*-
"""
Created on Tue Jan  2 20:52:18 2024

@author: grzeg
"""

import pandas as pd
from google.cloud import bigquery

# pobranie danych inflacyjnych

client = bigquery.Client()
 
project_id_1 = 'projekt-inwestycyjny'
dataset_id_1 = 'Inflation'
table_id_1 = 'Inflation'
destination_table_1 = f"{project_id_1}.{dataset_id_1}.{table_id_1}"

query_1 = """
    SELECT *
    FROM """ & {destination_table_1} & """
    """
    
query_job_1 = client.query(query_1)

# dane zakupowe i sprzedażowe - odczyt z pliku i połączenie
# pobranie danych zakupowo - sprzedażowych

project_id_2 = 'projekt-inwestycyjny'
dataset_id_2 = 'Transactions'
table_id_2 = 'Daily'
destination_table_2 = f"{project_id_2}.{dataset_id_2}.{table_id_2}"

query_2 = """
    SELECT *
    FROM """ & {destination_table_2} & """
    """
    
query_job_2 = client.query(query_2)



path = "D:\\Inwestowanie,banki\\Inwestowanie\\Portfel inwestycyjny\Portfel inwestycyjny 1.16.xlsm"
dane = pd.read_excel(io=path, sheet_name="Dane")

tabela_marz = pd.read_excel(io = path, sheet_name = "Marże obligacji skarbowych")

dane_instrumentów = pd.read_excel(io=path, sheet_name = "Lista_instrumentów")

dane_instrumentów = dane_instrumentów.query('Rodzaj == "Obligacje skarbowe"')
ticker_list_available = list(dane_instrumentów['Ticker'])

dane_obligacji = dane.merge(right=dane_instrumentów, how='inner', \
                            left_on = 'Ticker ID', right_on = 'Ticker_ID').merge\
    (right=tabela_marz, how='inner', left_on = 'Ticker_x', right_on = 'Ticker')
    
    
dane_obligacji['Data'] = pd.to_datetime(dane_obligacji['Data'], format='%Y-%m-%d').dt.date


ticker_list = (dane_obligacji['Ticker_x'])
dates_list = (dane_obligacji['Data'])
wolumen_list = (dane_obligacji['Wolumen'])
marza_pierwszy_rok_list = (dane_obligacji['Marża pierwszy rok'])
marza_kolejne_lata_list = (dane_obligacji['Marża kolejne lata'])

dane_do_analizy = pd.concat([ticker_list, 
                   dates_list, 
                   wolumen_list,
                   marza_pierwszy_rok_list,
                   marza_kolejne_lata_list], 
                   axis=1)


# analityka - szukanie wartosci aktualnej
result_df = pd.DataFrame(columns=['Ticker', 'Date', 'Current Value'])
for dane in dane_do_analizy.iterrows():
    ticker = dane[1][0]
    data_zakupu = dane[1][1]
    wolumen = dane[1][2]
    marza_pierwszy_rok = dane[1][3]
    marza_kolejne_lata = dane[1][4]
    
# input data
# data_zakupu = date(2023, 1, 1)
# wolumen = 10
# marza_pierwszy_rok = 6.75
# marza_kolejne_lata = 1.75


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
        print(f"Obligacje zostały zakupione w dniu {data_zakupu} na wolumen {wolumen} sztuk.")
        print(f"Marża w pierwszym roku wyniosła {marza_pierwszy_rok}%.")
        print(f"Wartosc inwestycji, po pierwszym roku to {current_value} zł.\n")
        for i in range(liczba_lat, 0, -1):
            liczba_dni_przesuniecie = timedelta(days= 365 * n - 60)
            data_badania_inflacji = date((data_zakupu + liczba_dni_przesuniecie).year, \
                                 (data_zakupu + liczba_dni_przesuniecie).month, \
                                 1)
        
            inflacja = df.loc[df['Początek miesiąca'] == data_badania_inflacji]\
                .iloc[:,:1]
            inflacja['Inflacja'] = inflacja['Inflacja'].str.replace(',','.')
            inflacja = float(inflacja.iloc[0,0])
            print(f"{n}. Inflacja została zbadana dnia {data_badania_inflacji} i wynosi {inflacja}%.")
            if liczba_dni < 730:
                current_value = current_value + current_value * \
                    (liczba_dni - 365)/365 * (inflacja + marza_kolejne_lata)/ 100
                print(f"Oprocentowanie w tym okresie wynosi {inflacja + marza_kolejne_lata}%.")
                print(f"Wartosc po tym okresie wynosi {round(current_value,2)} zł.\n")
            else:
                current_value = current_value + current_value * \
                    (inflacja + marza_kolejne_lata) / 100
                liczba_dni = liczba_dni - 365
                print(f"Oprocentowanie w tym okresie wynosi {inflacja + marza_kolejne_lata}%.")
                print(f"Wartosc po tym okresie wynosi {round(current_value,2)} zł.\n")
            n = n + 1 
            
    print(f"Wartosc w dniu obecnym dla tickera {ticker} wynosi {round(current_value, 2)} zł.")
    result_df = pd.concat([result_df, \
                           pd.DataFrame(data=[[ticker, data_zakupu, \
                                               round(current_value, 2)]], \
                                        columns=['Ticker', 'Date', 'Current Value'])])
    result_dfs = result_df.merge(right=dane_obligacji, 
                    how='inner',
                    left_on=['Ticker', 'Date'],
                    right_on= ['Ticker', 'Data'])


    saving_path = "D:\Inwestowanie,banki\Inwestowanie\Obligacje- analityka\Wyniki obligacji skarbowych.xlsx"
    result_dfs.to_excel(excel_writer=saving_path, sheet_name='Obligacje skarbowe',
                        columns=['Ticker', 'Ticker ID', 'Waluta', 'Data', \
                                  'Wolumen', 'Wartość_bez_prowizji',\
                                      'Current Value',\
                                      'Kurs waluty transakcji'],
                        index=False)
