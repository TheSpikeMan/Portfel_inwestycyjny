/*
OPIS WIDOKU

W widoku zawarte są transakcje wszystkich instrumentów, które zostały sprzedane i nie znajdują się obecnie w portfelu.
*/

WITH 
transaction_view AS (SELECT * FROM `projekt-inwestycyjny.Transactions.Transactions_view`),

-- INITIAL VIEW --
/*
W pierwszym kroku pobrane są wszystkie transakcje (sprzedaże lub zakupy), a następnie zostaje dokonane działanie:
- dla wszystkich sprzedaży zamień wolumen wartości sprzedanej na wartość ze znakiem ujemnym,
- dla wszystkich zakupów nie rób nic.

Dzięki temu możliwe jest wyznaczenie instrumentów sprzedanych.
*/

initial_view AS (
  SELECT
    * EXCEPT(Transaction_amount),
    CASE
      WHEN Transaction_type = 'Buy' THEN Transaction_amount
      WHEN Transaction_type = 'Sell' THEN (-1) * Transaction_amount
      ELSE Transaction_amount
      END AS Transaction_amount
  FROM
    transaction_view
  WHERE
    Transaction_type IN ('Buy', 'Sell')
),

-- TICKERS SOLD VIEW --
/*
W tym kroku wyznaczone są tickery sprzedane na podstawie warunku: jeżeli instrument ma sumę wolumenu = 0 to znaczy, że jest sprzedany.
*/

tickers_sold_view AS(
  SELECT
    Ticker,
    SUM(Transaction_amount) AS suma_wolumenu
  FROM initial_view
  GROUP BY
    Ticker
  HAVING
    suma_wolumenu = 0
),


-- MID VIEW --
/*
W tym kroku wyznaczane są wszystkie dane transakcyjne, dla instrumentów, które zostały sprzedane.
*/

mid_view AS (
  SELECT
    * EXCEPT (suma_wolumenu, Ticker),
    tickers_sold_view.Ticker AS Ticker
  FROM transaction_view
  INNER JOIN tickers_sold_view
  ON transaction_view.Ticker = tickers_sold_view.Ticker
)

SELECT *
FROM mid_view