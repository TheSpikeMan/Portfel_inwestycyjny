MERGE `my_dataset.stock_data` T -- # 1 Nazwa tabeli docelowej, do wstawienia
USING `my_dataset.stock_data_new` S -- # 2 Nazwa tabeli z nowymi danymi, do wstawienia
ON T.Ticker = S.Ticker AND T.Date = S.Date AND T.Metric = S.Metric AND T.Source = S.Source

-- Tylko dla danych kwartalnych i rocznych: aktualizuj, jeśli wartość się zmieniła
WHEN MATCHED
  AND S.Source IN ('Financials Quarterly', 'Financials Annual')
  AND T.Value IS DISTINCT FROM S.Value
THEN UPDATE SET Value = S.Value

-- Wstaw dane z wszystkich źródeł, które nie istnieją jeszcze w tabeli
WHEN NOT MATCHED THEN
  INSERT (Ticker, Date, Metric, Value, Source)
  VALUES (S.Ticker, S.Date, S.Metric, S.Value, S.Source)
