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
  WHERE Transaction_type <> 'Dywidenda'
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
            ticker = transaction[1]['Ticker']
            ticker_id = transaction[1]['Instrument_id']
            currency_type = transaction[1]['Currency']
            date_sold = transaction[1]['Transaction_date']
            
            # 9. W amount_sold pobieram nie dane ilosci sprzedane in total
            # ale ilosci aktualne pozostałe nierozliczone.
            amount_sold = transactions_df.loc[index, 'amount_location']
            ticker_search = ticker
            price_sold = transaction[1]['Transaction_price']
            currency_sold = transaction[1]['Currency_close']


            # 10. Szukaj takiego rekordu, dla którego znajdziesz transakcję
            # zakupu danego instrumentu, dla którego pozostała ilość
            # nie jest rowna 0. W innym wypadku tak długo dodawaj 1
            # do parametru k, aż znajdziesz to czego szukasz.
          
            while not((transactions_df.loc[k, 'Transaction_type'] == "Buy") and \
                         (transactions_df.loc[k, 'amount_location'] != 0) and \
                         (transactions_df.loc[k, 'Ticker'] == ticker_search)):
                k = k + 1

            # 11. Do zmiennych przypisz odpowiednie wartości na podstawie konkretnej transakcji zakupowej.
            amount_bought = transactions_df.loc[k, 'amount_location']
            date_bought = transactions_df.loc[k, 'Transaction_date']
            price_bought = transactions_df.loc[k, 'Transaction_price']
            currency_bought = transactions_df.loc[k, 'Currency_close']

              
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

            data_to_add = [date_sold, date_bought, (date_sold-date_bought).days,
                               Amount, price_sold, price_bought, currency_sold,
                               currency_bought, currency_type, ticker,
                               ticker_id, Amount * price_bought * currency_bought,
                               Amount * price_sold * currency_sold,
                               Amount * (price_sold * currency_sold - price_bought * currency_bought)]
            
            # 16. Dodaj do biężącej DataFrame dane z tablicy.
            result_df = pd.concat([result_df, pd.DataFrame([data_to_add])], 
                                     axis = 0)
    else:
        # 17. Jeżeli nie znajdziesz transakcji sprzedaży, szukaj dalej, aż znajdziesz.

        continue
  
# 18. Zmień nazwy kolumn na odpowiednie, zdefiniowane poniżej.
    
columns = ['Data_sprzedaży', 'Data_zakupu',	'Czas_inwestycji',	'Wolumen',
           'Cena_sprzedaży', 'Cena_zakupu', 'Kurs_sprzedaży',
           'Kurs_zakupu', 'Waluta', 'Ticker', 'Ticker_ID',
           'Koszt_uzyskania_przychodu',	'Przychód',	'Dochód']
result_df.columns = columns
print("Program zakończył się poprawnie.")
