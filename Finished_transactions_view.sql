/*
OPIS WIDOKU

W widoku zawarte są transakcje wszystkich instrumentów, które zostały sprzedane.
*/

WITH 
transactions_view_raw AS (SELECT * FROM `projekt-inwestycyjny.Transactions.Transactions_view`),

-- INITIAL VIEW --
/*
W pierwszym kroku pobrane są wszystkie tickery dla transakcji, które są zrealizowane (analiza ilościowa)
Dodatkowo wyznaczana jest maksymalna data zakończonej transakcji, aby dobrać dywidendy
*/

initial_view AS (
  SELECT DISTINCT
    Ticker                                      AS Ticker,
    MAX(Transaction_date) OVER ticker_window    AS max_transaction_date
  FROM transactions_view_raw
  WHERE TRUE
    AND transaction_date_buy_ticker_amount <= cumulative_sell_amount_per_ticker
    AND Transaction_type IN ('Buy', 'Sell', 'Wykup')
  WINDOW
    ticker_window AS (
      PARTITION BY Ticker
    )
),

-- all_finished_transactions_and_dividends--
/*
W widoku tym wyciągane są wszystkie tranakcje i dywidendy, które zrealizowane były w ramach sprzedanych już instrumentów.
*/

all_finished_transactions_and_dividends AS (
  SELECT 
    * EXCEPT(Ticker),
    tvr.Ticker                      AS Ticker,
    COALESCE(CASE WHEN Transaction_type_group = 'Buy_amount' THEN Transaction_value_pln ELSE 0 END, 0) 
                                    AS Buy_amount,
    COALESCE(CASE WHEN Transaction_type_group = 'Sell_amount' THEN Transaction_value_pln ELSE 0 END, 0) 
                                    AS Sell_amount,
    COALESCE(CASE WHEN Transaction_type_group = 'Div_related_amount' THEN Transaction_value_pln ELSE 0 END, 0) 
                                    AS Div_related_amount
  FROM transactions_view_raw        AS tvr
  INNER JOIN initial_view           AS iv
  ON tvr.Ticker                     = iv.Ticker
  WHERE TRUE
    AND iv.max_transaction_date     > tvr.Transaction_date
)

-- FINAL AGGREGATON --
/* 
W tym kroku wyciągane są:
- Data ostatniej transakcji,
- Skumulowana wartość zakupów,
- Skumulowana wartość sprzedaży,
- Skumulowany zysk z uwzględnieniem dywidend,
- Skumulowany zysk procentowy na danym instrumencie.
\
Wartość sprzedaży i inne wyciągane są z użyciem funkcji COALESCE - funkcja ta wybiera pierwszą nienulową wartość, więć jeśli kolumny Buy lub Sell przyjmuję NULL zastępuje je wartośćią 0.
*/

SELECT
  Ticker                                          AS Ticker,
  MAX(Transaction_date)                           AS Last_transaction_date,
  ROUND(SUM(COALESCE(Buy_amount, 0)), 2)          AS Cumulative_buy_value,
  ROUND(SUM(COALESCE(Sell_amount, 0)), 2)         AS Cumulative_sell_value,
  ROUND(SUM(COALESCE(Div_related_amount, 0)), 2)  AS Cumulative_dividend_value,
  ROUND(SUM(COALESCE(Sell_amount, 0)) - SUM(COALESCE(Buy_amount, 0)) + SUM(COALESCE(Div_related_amount, 0)), 2) 
                                                  AS profit_inlcuding_dividend,
  ROUND(100 * (SUM(COALESCE(Sell_amount, 0)) - SUM(COALESCE(Buy_amount, 0)) + SUM(COALESCE(Div_related_amount, 0)))/(SUM(COALESCE(Buy_amount, 0))), 2) 
                                                  AS profit_percentage
FROM all_finished_transactions_and_dividends
GROUP BY ALL
ORDER BY Ticker 


