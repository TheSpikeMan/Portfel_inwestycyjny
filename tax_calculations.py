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
)

SELECT * FROM Transactions_view

"""
query_job = client.query(query=query)
transactions_df = query_job.to_dataframe()

transactions_df['amount_location'] = transactions_df['Transaction_amount']

# Utworzenie wynikowego DataFrame

columns = ['Data_sprzedaży', 'Data_zakupu',	'Czas_inwestycji',	'Wolumen',
           'Cena_sprzedaży', 'Cena_zakupu', 'Kurs_sprzedaży',
           'Kurs_zakupu', 'Waluta', 'Ticker', 'Ticker_ID',
           'Koszt_uzyskania_przychodu',	'Przychód',	'Dochód']

result_df = pd.DataFrame(columns=columns)
k = 0



for index, transaction in enumerate(transactions_df.iterrows()):
    if (transaction[1]['Transaction_type']=="Sell") or (transaction[1]['Transaction_type']=="Wykup"):
        while(transaction[1]['amount_location'] != 0):
               ticker = transaction[1]['Ticker']
               ticker_id = transaction[1]['Instrument_id']
               currency_type = transaction[1]['Currency']
               
               
               date_sold = transaction[1]['Transaction_date']
               amount_sold = transaction[1]['Transaction_amount']
               ticker_search = ticker
               price_sold = transaction[1]['Transaction_price']
               currency_sold = transaction[1]['Currency_close']
               
               while not((transactions_df.loc[k, 'Transaction_type'] == "Buy") and \
                         (transactions_df.loc[k, 'amount_location'] != 0) and \
                         (transactions_df.loc[k, 'Ticker'] == ticker_search)):
                             k = k +1
               amount_bought = transactions_df.loc[k, 'amount_location']
               date_bought = transactions_df.loc[k, 'Transaction_date']
               price_bought = transactions_df.loc[k, 'Transaction_price']
               currency_bought = transactions_df.loc[k, 'Currency_close']
               
               if amount_sold > amount_bought:
                   Amount = amount_bought
                   transactions_df.loc[k, 'amount_location'] = 0
               else:
                   if amount_sold <= amount_bought:
                      Amount = amount_sold
                      transactions_df.loc[k, 'amount_location'] = amount_bought - \
                          amount_sold
               transactions_df.loc[k, 'amount_location'] = \
                   transactions_df.loc[k, 'amount_location'] - Amount
               k = 0
               
               data_to_add = [date_sold, date_bought, date_sold-date_bought,
                               Amount, price_sold, price_bought, currency_sold,
                               currency_bought, currency_type, ticker,
                               ticker_id, Amount * price_bought * currency_bought,
                               Amount * price_sold * currency_sold,
                               Amount * (price_sold * currency_sold - price_bought * currency_bought)]
               result_df = pd.concat([result_df, pd.DataFrame([data_to_add])], 
                                     axis = 0)
    else:
        continue
         
