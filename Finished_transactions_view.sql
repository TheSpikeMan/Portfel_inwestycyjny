/*
OPIS WIDOKU

W widoku zawarte są transakcje wszystkich instrumentów, które zostały sprzedane.
*/

WITH 
transactions_view AS (SELECT * FROM `projekt-inwestycyjny.Transactions.Transactions_view`),

-- INITIAL VIEW --
/*
W pierwszym kroku pobrane są wszystkie tickery dla transakcji, które są zrealizowane (analiza ilościowa.)
Dodatkowo wyznaczana jest maksymalna data zakończonej transakcji, aby dobrać dywidendy
*/

initial_view AS (
  SELECT DISTINCT
    Ticker                                      AS Ticker,
    MAX(Transaction_date) OVER ticker_window    AS max_transaction_date
  FROM transactions_view
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
    transactions_view.Ticker    AS Ticker
  FROM transactions_view
  INNER JOIN initial_view
  ON transactions_view.Ticker             = initial_view.Ticker
  WHERE TRUE
    AND initial_view.max_transaction_date > Transaction_date
),


-- INTERMEDIATE VIEW --
/*
W tym kroku pivotowana jest kolumna Transaction_Type
*/

intermediate_view AS (
  SELECT * 
  FROM all_finished_transactions_and_dividends
    PIVOT(SUM(Transaction_value_pln) 
      FOR Transaction_type_group IN ('Buy_amount', 'Sell_amount', 'Div_related_amount'))
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
  Ticker,
  MAX(Transaction_date)                           AS Last_transaction_date,
  ROUND(SUM(COALESCE(Buy_amount, 0)), 2)          AS Cumulative_buy_value,
  ROUND(SUM(COALESCE(Sell_amount, 0)), 2)         AS Cumulative_sell_value,
  ROUND(SUM(COALESCE(Div_related_amount, 0)), 2)  AS Cumulative_dividend_value,
  ROUND(SUM(COALESCE(Sell_amount, 0)) - SUM(COALESCE(Buy_amount, 0)) + SUM(COALESCE(Div_related_amount, 0)), 2) 
                                                  AS profit_inlcuding_dividend,
  ROUND(100 * (SUM(COALESCE(Sell_amount, 0)) - SUM(COALESCE(Buy_amount, 0)) + SUM(COALESCE(Div_related_amount, 0)))/(SUM(COALESCE(Buy_amount, 0))), 2) 
                                                  AS profit_percentage
FROM
  intermediate_view
GROUP BY
  Ticker
ORDER BY
  Ticker ASC


