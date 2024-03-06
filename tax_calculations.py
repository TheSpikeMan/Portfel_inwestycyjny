from google.cloud import bigquery
import pandas as pd

# 1. Pobranie danych transakcyjnych
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
  ORDER BY Transaction_id ASC
)

SELECT * FROM Transactions_view

"""
query_job = client.query(query=query)
transactions_df = query_job.to_dataframe()


# 2. Zdublowanie kolumny, dla celów dokonywania obliczeń na kolumnie
transactions_df['amount_location'] = transactions_df['Transaction_amount']

# 3. Utworzenie wynikowego pustego DataFrame
result_df = pd.DataFrame()

# 4. Zdefiniowanie parametru k, który będzie iterowalny wewnątrz pętli, w celu
# wyszuwania transakcji kupna danego tickera.
k = 0

# 5. Pętla wykonywalna na każdym obiekcie typu series (transaction)
for index, transaction in enumerate(transactions_df.iterrows()):
    
    # 6. W tym miejscu analizowane są wyłącznie transakcje o charakterystyce 'Sell' oraz 'Wykup'
    if (transaction[1]['Transaction_type']=="Sell") or (transaction[1]['Transaction_type']=="Wykup"):
        
        # 7. Wykonuj tę pętlę dopóki nie wyzerujesz łącznej ilości sprzedaży w danej transakcji.
        while(transactions_df.loc[index, 'amount_location'] != 0):
            
            # 8. Przypisz do zmiennych warości z obiektu Series.
            ticker              = transaction[1]['Ticker']
            ticker_id           = transaction[1]['Instrument_id']
            currency_type       = transaction[1]['Currency']
            date_sold           = transaction[1]['Transaction_date']
            
            # 9. W amount_sold pobieram nie dane ilosci sprzedane in total
            # ale ilosci aktualne pozostałe nierozliczone.
            amount_sold         = transactions_df.loc[index, 'amount_location']
            ticker_search       = ticker
            price_sold          = transaction[1]['Transaction_price']
            currency_sold       = transaction[1]['Currency_close']


            # 10. Szukaj takiego rekordu, dla którego znajdziesz transakcję
            # zakupu danego instrumentu, dla którego pozostała ilość
            # nie jest rowna 0. W innym wypadku tak długo dodawaj 1
            # do parametru k, aż znajdziesz to czego szukasz.
          
            while not((transactions_df.loc[k, 'Transaction_type'] == "Buy") and \
                         (transactions_df.loc[k, 'amount_location'] != 0) and \
                         (transactions_df.loc[k, 'Ticker'] == ticker_search)):
                k = k + 1

            # 11. Do zmiennych przypisz odpowiednie wartości na podstawie konkretnej transakcji zakupowej.
            amount_bought       = transactions_df.loc[k, 'amount_location']
            date_bought         = transactions_df.loc[k, 'Transaction_date']
            price_bought        = transactions_df.loc[k, 'Transaction_price']
            currency_bought     = transactions_df.loc[k, 'Currency_close']

              
            # 12. Zaktualizuj wartość transakcji zakupowej o dane ilściowe, zgodnie z algorytmem
            if amount_sold > amount_bought:
                Amount = amount_bought
                transactions_df.loc[k, 'amount_location'] = 0
            else:
                Amount = amount_sold
                
                transactions_df.loc[k, 'amount_location'] = amount_bought - \
                          amount_sold
            
            # 13. Na końcu zmniejsz również ilość w transakcji sprzedaży, o to co udało się odnaleźć
            # w transakcji zakupu (Amount)
            transactions_df.loc[index, 'amount_location'] = \
                   transactions_df.loc[index, 'amount_location'] - Amount
            
            # 14. Wyzeruj parametr k. Służy on do poruszania się po danych zakupowych w punkcie nr 10.
            k = 0
            
            # 15. Zbierz wszystkie dane do tablicy.

            data_to_add = [date_sold, 
                           date_bought, 
                           (date_sold-date_bought).days,
                           Amount,
                           price_sold, 
                           price_bought, 
                           currency_sold,
                           currency_bought, 
                           currency_type, 
                           ticker,
                           ticker_id, 
                           (Amount * price_bought * currency_bought).round(2),
                           (Amount * price_sold * currency_sold).round(2),
                           (Amount * (price_sold * currency_sold - price_bought * currency_bought)).round(2)
                           ]
            
            # 16. Dodaj do biężącej DataFrame dane z tablicy.
            result_df = pd.concat([result_df, pd.DataFrame([data_to_add])], 
                                     axis = 0)
            
    # 17. Jeżeli nie znajdziesz transakcji sprzedaży lub wykupu szukaj dywidend i odsetek.
    else: 
        if (transaction[1]['Transaction_type']=="Dywidenda") or (transaction[1]['Transaction_type']=="Odsetki"):
            dividend_interest_payment_date   = transaction[1]['Transaction_date']
            dividend_interest_amount         = transaction[1]['Transaction_amount']
            dividend_interest_value          = transaction[1]['Transaction_price']
            dividend_interest_currency_value = transaction[1]['Currency_close']
            dividend_interest_ticker         = transaction[1]['Ticker']
            dividend_interest_ticker_id      = transaction[1]['Instrument_id']
            dividend_interest_currency       = transaction[1]['Currency']

            # 18. Zbierz wszystkie dane dywidend do tablicy.

            data_to_add = [dividend_interest_payment_date, 
                        None, 
                        None,
                        dividend_interest_amount,
                        None,
                        dividend_interest_value, 
                        None,
                        dividend_interest_currency_value, 
                        dividend_interest_currency, 
                        dividend_interest_ticker,
                        dividend_interest_ticker_id, 
                        None,
                        round((dividend_interest_amount * dividend_interest_value * dividend_interest_currency_value), 2),
                        round((dividend_interest_amount * dividend_interest_value * dividend_interest_currency_value), 2)
                        ]
            
            # 16. Dodaj do biężącej DataFrame dane z tablicy.

            result_df = pd.concat([result_df, pd.DataFrame([data_to_add])], 
                                        axis = 0)


        else:
        # 17. Jeżeli nie znajdziesz transakcji sprzedaży lub dywidend, szukaj dalej, aż znajdziesz.

            continue
  
# 18. Zmień nazwy kolumn na odpowiednie, zdefiniowane poniżej.
    
columns = ['Date_sell', 'Date_buy',	'Investment_period',	'Quantity',
           'Buy_Price', 'Sell_Price', 'Buy_currency',
           'Sell_currency', 'Currency', 'Ticker', 'Ticker_id',
           'Tax_deductible_expenses',	'Income',	'Profit']
result_df.columns = columns

# 19. Zdefiniownie miejsca eksportu danych.

project_id = 'projekt-inwestycyjny'
dataset_id = 'Transactions'
table_id = 'Tax_calculations'
destination_table = f"{project_id}.{dataset_id}.{table_id}"

# 20. Przygotowanie schematu danych.
schema = [bigquery.SchemaField(name = 'Date_sell', field_type = "DATE", \
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
          bigquery.SchemaField(name = 'Ticker', field_type = "STRING",\
                                mode = "REQUIRED"),
          bigquery.SchemaField(name = 'Ticker_id', field_type = "INTEGER",\
                                mode = "REQUIRED"),
          bigquery.SchemaField(name = 'Tax_deductible_expenses', field_type = "FLOAT",\
                                mode = "NULLABLE"),
          bigquery.SchemaField(name = 'Income', field_type = "FLOAT",\
                                mode = "NULLABLE"),
          bigquery.SchemaField(name = 'Profit', field_type = "FLOAT",\
                                mode = "NULLABLE")
                                ]

# 21. Zdefiniowanie joba, który wykona operację eksportu.
job_config = bigquery.LoadJobConfig(schema = schema,
                                    write_disposition = "WRITE_TRUNCATE")

# 22. Wykonanie eksportu danych metodą load_table_from_dataframe na obiekcie client klasy Client.

try:
    job = client.load_table_from_dataframe(dataframe=result_df, 
                                            destination=destination_table,
                                            num_retries=1,
                                            job_config=job_config)
    job.result()
    print("Success exporting the data to BigQuery.")
except Exception as e:
    print(f"Error uploading data to BigQuery: {str(e)}")