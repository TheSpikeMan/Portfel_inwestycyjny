/*
OPIS WIDOKU

W widoku zawarte są transakcje wszystkich instrumentów, które zostały sprzedane.
*/

WITH 
transactions_view AS (SELECT * FROM `projekt-inwestycyjny.Transactions.Transactions_view`),

-- INITIAL VIEW --
/*
W pierwszym kroku pobrane są wszystkie transakcje (sprzedaże lub zakupy).
Wykluczenie Tickera w klauzuli SELECT wynika, z konieczności późniejszego łączenia z danymi dywinend i dostosowania kolejności kolumn.
Wyciągane są wszystkie transakcje, dla które całkowita zakupiona ilość jest mniejsza bądź równa całkowitej ilości sprzedaży. Dzięki temu udaje się wyciągnąć sprzedane instrumenty.

*/

initial_view AS (
  SELECT
    * EXCEPT (Ticker),
    Ticker
  FROM transactions_view
  WHERE TRUE
    AND transaction_date_buy_ticker_amount <= cumulative_sell_amount_per_ticker
    AND Transaction_type IN ('Buy', 'Sell')
),

-- MAX TRANSACTION DATES PER TICKER --
/*
W widoku tym wyciągane są maksymalne daty transakcji z poprzedniego widoku (INITIAL VIEW), w celu późniejszego dobrania takich dywidend, które były realizowane w czasie posiadania danego instrumentu w porfelu.
*/

max_transaction_dates_per_ticker AS (
  SELECT
    Ticker,
    MAX(Transaction_date) AS max_transaction_date
  FROM 
    initial_view
  GROUP BY
    Ticker
),


-- ALL DIVIDEND TRANSACTIONS WITHIN MAXIMUM DATES --
/*
W widoku tym wyciągane są wszystkie dywidendy, które zrealizowane były w ramach sprzedanych już instrumentów.
*/

all_dividend_transactions_within_maximum_dates AS (
  SELECT
    * EXCEPT (Ticker, max_transaction_date),
    transactions_view.Ticker
  FROM
    transactions_view
  LEFT JOIN max_transaction_dates_per_ticker
  ON transactions_view.Ticker = max_transaction_dates_per_ticker.Ticker
  WHERE
    Transaction_type = 'Dywidenda'
    AND max_transaction_date > Transaction_date
),

-- TRANSACTIONS PLUS DIVIDENDS --
/*
W widoku tym łączone są transakcje zakończone/zrealizowane wraz z wypłaconymi w tym czasie dywidendami.
*/
transactions_plus_dividends AS (
  SELECT *
  FROM initial_view

  UNION ALL

  SELECT *
  FROM all_dividend_transactions_within_maximum_dates
),

-- INTERMEDIATE VIEW --
/*
W tym kroku pivotowana jest kolumna Transaction_Type
*/

intermediate_view AS (
  SELECT *
  FROM transactions_plus_dividends
    PIVOT(SUM(Transaction_value_pln) FOR Transaction_type IN ('Buy', 'Sell', 'Dywidenda'))
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
  MAX(Transaction_date) AS Last_transaction_date,
  ROUND(SUM(COALESCE(Buy, 0)), 2) AS Cumulative_buy_value,
  ROUND(SUM(COALESCE(Sell, 0)), 2) AS Cumulative_sell_value,
  ROUND(SUM(COALESCE(Dywidenda, 0)), 2) AS Cumulative_dividend_value,
  ROUND(SUM(COALESCE(Sell, 0)) - SUM(COALESCE(Buy, 0)) + SUM(COALESCE(Dywidenda, 0)), 2) AS profit_inlcuding_dividend,
  ROUND(100 * (SUM(COALESCE(Sell, 0)) - SUM(COALESCE(Buy, 0)) + SUM(COALESCE(Dywidenda, 0)))/(SUM(COALESCE(Buy, 0))), 2) AS profit_percentage
FROM
  intermediate_view
GROUP BY
  Ticker
ORDER BY
  Ticker ASC


