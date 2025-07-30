from google.cloud import bigquery
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
import os

# 1. Pobieram dane transakcyjne
client = bigquery.Client()

# Zmienne środowiskowe
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

# Definicja nazwy projektu
project = os.getenv("BQ_PROJECT_ID")
# Definicje nazw datasetu
dataSetTransactions = os.getenv("BQ_DATASET_TRANSACTIONS")
# Definicje nazw widoku
viewTransactionsView = os.getenv("BQ_VIEW_TRANSACTIONS_VIEW")

# Zapytanie do bazy dnaych
query = f"""
SELECT * FROM {project}.{dataSetTransactions}.{viewTransactionsView}
"""

# Utworzenie obiektu klasy QueryJob na podstawie zapytania
query_job = client.query(query=query)

# Konwersja obiektu QueryJob na DataFrame
transactions_df = query_job.to_dataframe()

# 2. Zdublowanie kolumny, dla celów dokonywania obliczeń na kolumnie
transactions_df['amount_location'] = transactions_df['Transaction_amount']

# 3. Utworzenie pustej listy
list_to_append = []

# 4. Zdefiniowanie parametru k, który będzie iterowalny wewnątrz pętli, w celu
# wyszukiwania transakcji kupna danego instrumentu finansowego.
k = 0

# 5. Zdefiniowanie parametru l, który przechowuje ilość transakcji zakupowych dla danego
# instrumentu finansowego - zostanie on użyty do równomiernego rozdzielenia prowizji przy sprzedaży instrumentu
# wobec transakcji zakupowych.
l = 0 

# 5. Pętla wykonywalna na każdym obiekcie typu series (transaction)
for index, transaction in enumerate(transactions_df.iterrows()):
    
    # 6. W tym miejscu analizowane są wyłącznie transakcje o charakterystyce 'Sell' oraz 'Wykup'
    if (transaction[1]['Transaction_type']=="Sell") or (transaction[1]['Transaction_type']=="Wykup"):
        
        # 7. Wykonuj tę pętlę dopóki nie wyzerujesz łącznej ilości sprzedaży w danej transakcji.
        while(transactions_df.loc[index, 'amount_location'] != 0):

            # 8. Przypisz do zmiennych wartości z obiektu Series.
            project_id          = transaction[1]['Project_id']
            ticker              = transaction[1]['Ticker']
            ticker_id           = transaction[1]['Instrument_id']
            currency_type       = transaction[1]['Currency']
            date_sold           = transaction[1]['Transaction_date']
            tax_paid            = transaction[1]['Tax_paid']
            tax_value           = transaction[1]['Tax_value']
            transaction_type    = transaction[1]['Transaction_type']
            
            # 9. W amount_sold pobieram nie dane ilosci sprzedane in total
            # ale ilosci aktualne pozostałe nierozliczone.
            # amount_sold_total to całkowita ilość sprzedaży danego instrumentu w ramach danej transakcji.

            amount_sold         = transactions_df.loc[index, 'amount_location']
            amount_sold_total   = transaction[1]['Transaction_amount']
            ticker_search       = ticker
            price_sold          = transaction[1]['Transaction_price']
            currency_sold       = transaction[1]['last_currency_close']
            commision_sold      = transaction[1]['Commision_id']
            instrument_type     = transaction[1]['Instrument_type']
            country             = transaction[1]['country']
            instrument_headquarter = transaction[1]['instrument_headquarter']
            

            # 10. Szukaj takiego rekordu, dla którego znajdziesz transakcję
            # zakupu danego instrumentu, dla którego pozostała ilość
            # nie jest rowna 0. W innym wypadku tak długo dodawaj 1
            # do parametru k, aż znajdziesz to czego szukasz.
          
            while not((transactions_df.loc[k, 'Transaction_type'] == "Buy") and \
                         (transactions_df.loc[k, 'amount_location'] != 0) and \
                         (transactions_df.loc[k, 'Project_id'] == project_id) and \
                         (transactions_df.loc[k, 'Ticker'] == ticker_search)):
                k = k + 1

            l = l + 1

            # 11. Do zmiennych przypisz odpowiednie wartości na podstawie konkretnej transakcji zakupowej.
            amount_bought       = transactions_df.loc[k, 'amount_location']
            date_bought         = transactions_df.loc[k, 'Transaction_date']

            # 12. W zależności od rodzaju transakcji wykorzystywane są odpowiednie kolumny z ceną.
            if transaction[1]['Instrument_type'] != "Obligacje korporacyjne":
                price_bought    = transactions_df.loc[k, 'Transaction_price']   
            elif transaction[1]['Instrument_type'] == "Obligacje korporacyjne":
                price_bought    = transactions_df.loc[k, 'Dirty_bond_price']

            currency_bought     = transactions_df.loc[k, 'last_currency_close']

            # 13. Dodanie wartości zapłaconej prowizji przy transakcji zakupu.
            commision_buy_paid  = transactions_df.loc[k, 'Commision_id']

            # 14. Dodanie wartości prowizji sprzedaży (obliczana na podstawie udziału danego zakupu w sprzedaży)
            commision_sell_paid = (commision_sold * amount_bought) / amount_sold_total

              
            # 15. Zaktualizuj wartość transakcji zakupowej o dane ilściowe, zgodnie z algorytmem
            if amount_sold > amount_bought:
                Amount = amount_bought
                transactions_df.loc[k, 'amount_location'] = 0
            else:
                Amount = amount_sold
                
                transactions_df.loc[k, 'amount_location'] = amount_bought - \
                          amount_sold
            
            # 16. Na końcu zmniejsz również ilość w transakcji sprzedaży, o to co udało się odnaleźć
            # w transakcji zakupu (Amount)
            transactions_df.loc[index, 'amount_location'] = \
                   transactions_df.loc[index, 'amount_location'] - Amount
            
            # 17. Wyzeruj parametr k. Służy on do poruszania się po danych zakupowych w punkcie nr 10.
            k = 0
            
            # 18. Zbierz wszystkie dane do tablicy.

            data_to_add = [project_id,
                           date_sold,
                           date_bought, 
                           (date_sold-date_bought).days,
                           Amount,
                           price_bought,
                           price_sold, 
                           currency_sold,
                           currency_bought, 
                           currency_type, 
                           transaction_type,
                           instrument_type,
                           country,
                           instrument_headquarter,
                           ticker,
                           ticker_id, 
                           round((Amount * price_bought * currency_bought + commision_buy_paid  + commision_sell_paid), 2),
                           round((Amount * price_sold * currency_sold), 2),
                           round((Amount * price_sold * currency_sold).round(2) - 
                                (Amount * price_bought * currency_bought + commision_buy_paid  + commision_sell_paid), 2),
                           tax_paid,
                           tax_value
                           ]
            
            # 19. Dodaj do biężącej DataFrame dane z tablicy.
            list_to_append.append(data_to_add)
            
    # 20. Jeżeli nie znajdziesz transakcji sprzedaży lub wykupu szukaj dywidend i odsetek.
    else: 
        if (transaction[1]['Transaction_type']=="Dywidenda") or (transaction[1]['Transaction_type']=="Odsetki"):
            project_id                       = transaction[1]['Project_id']
            dividend_interest_payment_date   = transaction[1]['Transaction_date']
            dividend_interest_amount         = transaction[1]['Transaction_amount']
            dividend_interest_value          = transaction[1]['Transaction_price']
            dividend_interest_currency_value = transaction[1]['last_currency_close']
            dividend_interest_ticker         = transaction[1]['Ticker']
            dividend_interest_ticker_id      = transaction[1]['Instrument_id']
            dividend_interest_currency       = transaction[1]['Currency']
            tax_paid                         = transaction[1]['Tax_paid']
            tax_value                        = transaction[1]['Tax_value']
            transaction_type                 = transaction[1]['Transaction_type']
            instrument_type                  = transaction[1]['Instrument_type']
            country                          = transaction[1]['country']
            instrument_headquarter           = transaction[1]['instrument_headquarter']

            # 19. Zbierz wszystkie dane dywidend do tablicy.

            data_to_add = [
                        project_id,
                        dividend_interest_payment_date,
                        None, 
                        None,
                        dividend_interest_amount,
                        None,
                        dividend_interest_value, 
                        None,
                        dividend_interest_currency_value, 
                        dividend_interest_currency,
                        transaction_type,
                        instrument_type,
                        country,
                        instrument_headquarter,
                        dividend_interest_ticker,
                        dividend_interest_ticker_id, 
                        None,
                        round((dividend_interest_amount * dividend_interest_value * dividend_interest_currency_value), 2),
                        round((dividend_interest_amount * dividend_interest_value * dividend_interest_currency_value), 2),
                        tax_paid,
                        tax_value
                        ]
            
            # 21. Dodaj do biężącej DataFrame dane z tablicy.

            list_to_append.append(data_to_add)


        else:
        # 22. Jeżeli nie znajdziesz transakcji sprzedaży lub dywidend, szukaj dalej, aż znajdziesz.

            continue
  
# 23. Zmień nazwy kolumn na odpowiednie, zdefiniowane poniżej.
    
columns = ['Project_id', 'Date_sell', 'Date_buy',	'Investment_period',	'Quantity',
           'Buy_Price', 'Sell_Price', 'Buy_currency',
           'Sell_currency', 'Currency', 'Transaction_type', 'Instrument_type', 'Country', 
           'Instrument_headquarter', 'Ticker', 'Ticker_id',
           'Tax_deductible_expenses',	'Income',	'Profit', 'Tax_paid', 'Tax_value']

# 24. Utworzenie Df na podstawie zestawu list.
result_df = pd.DataFrame(data=list_to_append, columns = columns)

# 25. Zdefiniownie miejsca eksportu danych.

project_id = 'projekt-inwestycyjny'
dataset_id = 'Transactions'
table_id = 'Tax_calculations'
destination_table = f"{project_id}.{dataset_id}.{table_id}"

# 26. Przygotowanie schematu danych.
schema = [bigquery.SchemaField(name = 'Project_id', field_type = "INTEGER", \
                                mode = "REQUIRED"),
          bigquery.SchemaField(name = 'Date_sell', field_type = "DATE", \
                                mode = "REQUIRED"),
          bigquery.SchemaField(name = 'Date_buy', field_type = "DATE",\
                                mode = "NULLABLE"),
          bigquery.SchemaField(name = 'Investment_period', field_type = "INTEGER",\
                                mode = "NULLABLE"),
          bigquery.SchemaField(name = 'Quantity', field_type = "INTEGER",\
                                mode = "REQUIRED"),
          bigquery.SchemaField(name = 'Buy_Price', field_type = "FLOAT",\
                                mode = "NULLABLE"),
          bigquery.SchemaField(name = 'Sell_Price', field_type = "FLOAT",\
                                mode = "REQUIRED"),
          bigquery.SchemaField(name = 'Buy_currency', field_type = "FLOAT",\
                                mode = "NULLABLE"),
          bigquery.SchemaField(name = 'Sell_currency', field_type = "FLOAT",\
                                mode = "REQUIRED"),
          bigquery.SchemaField(name = 'Currency', field_type = "STRING",\
                                mode = "NULLABLE"),
          bigquery.SchemaField(name = 'Transaction_type', field_type = "STRING",\
                                mode = "REQUIRED"),
          bigquery.SchemaField(name = 'Instrument_type', field_type = "STRING",\
                                mode = "REQUIRED"),
          bigquery.SchemaField(name = 'Country', field_type = "STRING",\
                                mode = "REQUIRED"),
          bigquery.SchemaField(name = 'Instrument_headquarter', field_type = "STRING",\
                                mode = "REQUIRED"),
          bigquery.SchemaField(name = 'Ticker', field_type = "STRING",\
                                mode = "REQUIRED"),
          bigquery.SchemaField(name = 'Ticker_id', field_type = "INTEGER",\
                                mode = "REQUIRED"),
          bigquery.SchemaField(name = 'Tax_deductible_expenses', field_type = "FLOAT",\
                                mode = "NULLABLE"),
          bigquery.SchemaField(name = 'Income', field_type = "FLOAT",\
                                mode = "NULLABLE"),
          bigquery.SchemaField(name = 'Profit', field_type = "FLOAT",\
                                mode = "NULLABLE"),
          bigquery.SchemaField(name = 'Tax_paid', field_type = "BOOL",\
                                mode = "REQUIRED"),
          bigquery.SchemaField(name = 'Tax_value', field_type = "FLOAT",\
                                mode = "NULLABLE")
                                ]

# 27. Zdefiniowanie joba, który wykona operację eksportu.
job_config = bigquery.LoadJobConfig(schema = schema,
                                    write_disposition = "WRITE_TRUNCATE")

# 28. Wykonanie eksportu danych metodą load_table_from_dataframe na obiekcie client klasy Client.

try:
    job = client.load_table_from_dataframe(dataframe=result_df, 
                                            destination=destination_table,
                                            num_retries=1,
                                            job_config=job_config)
    job.result()
    print("Success exporting the data to BigQuery.")
except Exception as e:
    print(f"Error uploading data to BigQuery: {str(e)}")