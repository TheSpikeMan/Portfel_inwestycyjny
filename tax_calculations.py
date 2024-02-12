# -*- coding: utf-8 -*-
"""
Created on Thu Jan 25 19:25:17 2024

@author: grzeg
"""

from google.cloud import bigquery
import pandas as pd

# Pobranie danych transakcyjnych
client = bigquery.Client()

project = 'projekt-inwestycyjny'
dataset = 'Transactions'
table = 'Transactions_view'

query = f"""
WITH
Transactions_view AS (
  SELECT
    *,
  FROM `{project}.{dataset}.{table}`
  WHERE Transaction_type <> 'Dywidenda'
  ORDER BY Transaction_id ASC
)

SELECT * FROM Transactions_view

"""
query_job = client.query(query=query)
transactions_df = query_job.to_dataframe()

transactions_df['amount_location'] = transactions_df['Transaction_amount']

# Utworzenie wynikowego DataFrame

result_df = pd.DataFrame()
k = 0



for index, transaction in enumerate(transactions_df.iterrows()):
    if (transaction[1]['Transaction_type']=="Sell") or (transaction[1]['Transaction_type']=="Wykup"):
        print(transactions_df.loc[index, 'amount_location'])
        while(transactions_df.loc[index, 'amount_location'] != 0):
        # while(transaction[1]['amount_location'] != 0):
            print(transactions_df.loc[index, 'amount_location'])
            ticker = transaction[1]['Ticker']
            ticker_id = transaction[1]['Instrument_id']
            currency_type = transaction[1]['Currency']
            date_sold = transaction[1]['Transaction_date']
            
            # W amount_sold pobieram nie dane ilosci sprzedane in total
            # ale ilosci aktualne pozostałe nierozliczone.
            amount_sold = transactions_df.loc[index, 'amount_location']
            ticker_search = ticker
            price_sold = transaction[1]['Transaction_price']
            currency_sold = transaction[1]['Currency_close']
              
            # Szukaj takiego rekordu, dla którego znajdziesz transakcję
            # zakupu danego instrumentu, dla którego pozostała ilosc 
            # nie jest rowna 0. W innym wypadku tak długo dodawaj 1
            # do parametru k, aż znajdziesz to czego szukasz
            
            print("Transaction type: ", transactions_df.loc[k, 'Transaction_type'])
            print("Amount location: ", transactions_df.loc[k, 'amount_location'] != 0)
            print("Ticker aktualny: ", transactions_df.loc[k, 'Ticker'])
    
            while not((transactions_df.loc[k, 'Transaction_type'] == "Buy") and \
                         (transactions_df.loc[k, 'amount_location'] != 0) and \
                         (transactions_df.loc[k, 'Ticker'] == ticker_search)):
                print("Współczynnik k wynosi: ", k)
                print("Ticker aktualny wynosi ", transactions_df.loc[k, 'Ticker'])
                k = k + 1
            amount_bought = transactions_df.loc[k, 'amount_location']
            date_bought = transactions_df.loc[k, 'Transaction_date']
            price_bought = transactions_df.loc[k, 'Transaction_price']
            currency_bought = transactions_df.loc[k, 'Currency_close']
            
            print("Amount sold wynosi: ", amount_sold)
            print("Amount_bought wynosi: ", amount_bought)
              
            print("Jestem na etapie amount_sold")
            if amount_sold > amount_bought:
                Amount = amount_bought
                transactions_df.loc[k, 'amount_location'] = 0
            else:
                Amount = amount_sold
                
                # Zmniejsza wartosc obecnego wolumenu danych zakupowych
                # o wartosc sprzedana
                transactions_df.loc[k, 'amount_location'] = amount_bought - \
                          amount_sold
            transactions_df.loc[index, 'amount_location'] = \
                   transactions_df.loc[index, 'amount_location'] - Amount
            k = 0
              
            print("Jestem na etapie data_to_add")
            data_to_add = [date_sold, date_bought, (date_sold-date_bought).days,
                               Amount, price_sold, price_bought, currency_sold,
                               currency_bought, currency_type, ticker,
                               ticker_id, Amount * price_bought * currency_bought,
                               Amount * price_sold * currency_sold,
                               Amount * (price_sold * currency_sold - price_bought * currency_bought)]
            result_df = pd.concat([result_df, pd.DataFrame([data_to_add])], 
                                     axis = 0)
            print("Przeszedłem całosc")
            print("Amount location wynosi: ", transaction[1]['amount_location'])
    else:
        continue
    
columns = ['Data_sprzedaży', 'Data_zakupu',	'Czas_inwestycji',	'Wolumen',
           'Cena_sprzedaży', 'Cena_zakupu', 'Kurs_sprzedaży',
           'Kurs_zakupu', 'Waluta', 'Ticker', 'Ticker_ID',
           'Koszt_uzyskania_przychodu',	'Przychód',	'Dochód']
result_df.columns = columns
