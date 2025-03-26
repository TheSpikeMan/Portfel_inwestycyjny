/*
W widoku zawarte są transakcje wszystkich instrumentów, które zostały sprzedane.
*/

WITH 
transactions_view_raw AS (SELECT * FROM `projekt-inwestycyjny.Transactions.Transactions_view`),

-- transactions_view --
/*
Wyciągnięcie danych transakcyjnych wraz z wyznaczeniem dla wszystkich transakcji wolumenu sprzedanego.
Transakcje sprzedażowe obecne są w całości.
Dywidendy i odsetki nieobecne.
*/

transactions_view AS (
  SELECT
    Project_id                    AS Project_id,
    Transaction_date              AS Transaction_date,
    Ticker                        AS Ticker,
    Transaction_type_group        AS Transaction_type_group,
    Transaction_price             AS Transaction_price,
    Currency_close                AS Currency_close,
    CASE
      -- Przypadek całkowitej sprzedaży danej transakcji zakupowej
      WHEN Transaction_type_group = "Buy_amount"
      AND transaction_date_buy_ticker_amount - cumulative_sell_amount_per_ticker <= 0
      THEN Transaction_amount

      -- Przypadek częściowej sprzedaży danej transakcji zakupowej
      WHEN Transaction_type_group = "Buy_amount"
      AND transaction_date_buy_ticker_amount - cumulative_sell_amount_per_ticker > 0
      AND 
        (
          ROW_NUMBER() OVER first_ticker_transaction_window = 1
          OR LAG(Transaction_date_buy_ticker_amount) OVER first_ticker_transaction_window < cumulative_sell_amount_per_ticker
        )
      THEN COALESCE(cumulative_sell_amount_per_ticker - SUM(Transaction_amount) OVER last_ticker_transaction_window, cumulative_sell_amount_per_ticker) 

      -- Rozważam wszystkie przypadki sprzedaży
      WHEN Transaction_type_group = "Sell_amount"
      THEN Transaction_amount

      -- Chociażby przypadki, gdy analizowana transakcja nie została całkowicie zrealizowana, ale nastąpiła
      -- jakakolwiek sprzedaż lub Dywidendy/Odsetki
      ELSE 0
      END                         AS amount_sold,
  FROM transactions_view_raw
  WINDOW
    last_ticker_transaction_window AS (
      PARTITION BY
        Project_id,
        Ticker,
        Transaction_type_group
      ORDER BY
        Transaction_date,
        Transaction_id
      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ),
    first_ticker_transaction_window AS (
      PARTITION BY
        Project_id,
        Ticker,
        Transaction_type_group
      ORDER BY
        Transaction_date,
        Transaction_id
    )
),

-- all_finished_transactions_and_dividends--
/*
W widoku tym wyciągane są wszystkie tranakcje i dywidendy, które zrealizowane były w ramach sprzedanych już instrumentów.
*/

all_finished_transactions AS (
  SELECT 
    transactions_view.Project_id                     AS Project_id,
    Ticker                                           AS Ticker,
    Transaction_date                                 AS Transaction_date,
    COALESCE(
      CASE WHEN Transaction_type_group = 'Buy_amount' 
      THEN amount_sold * Transaction_price * Currency_close ELSE 0 END, 0)      AS Buy_amount,
    COALESCE(
      CASE WHEN Transaction_type_group = 'Sell_amount' 
      THEN amount_sold * Transaction_price * Currency_close ELSE 0 END, 0)      AS Sell_amount,
    COALESCE(
      CASE WHEN Transaction_type_group = 'Div_related_amount' 
      THEN amount_sold * Transaction_price * Currency_close ELSE 0 END, 0)      AS Div_related_amount
  FROM transactions_view
  WHERE TRUE
    AND amount_sold <> 0 
)

-- FINAL AGGREGATON --
/* 
W tym kroku wyciągane są:
- Data ostatniej transakcji,
- Skumulowana wartość zakupów,
- Skumulowana wartość sprzedaży,
- Skumulowany zysk z uwzględnieniem dywidend -> DO DOROBIENIA
- Skumulowany zysk procentowy na danym instrumencie.
*/

SELECT
  Project_id                                      AS Project_id,                   
  Ticker                                          AS Ticker,
  MAX(Transaction_date)                           AS Last_transaction_date,
  ROUND(SUM(COALESCE(Buy_amount, 0)), 2)          AS Cumulative_buy_value,
  ROUND(SUM(COALESCE(Sell_amount, 0)), 2)         AS Cumulative_sell_value,
  ROUND(SUM(COALESCE(Div_related_amount, 0)), 2)  AS Cumulative_dividend_value,
  ROUND(SUM(COALESCE(Sell_amount, 0)) - SUM(COALESCE(Buy_amount, 0)) + SUM(COALESCE(Div_related_amount, 0)), 2) 
                                                  AS profit_including_dividend,
  ROUND(100 * SAFE_DIVIDE(SUM(COALESCE(Sell_amount, 0)) - SUM(COALESCE(Buy_amount, 0)) + SUM(COALESCE(Div_related_amount, 0)), (SUM(COALESCE(Buy_amount, 0)))), 2) 
                                                  AS profit_percentage
FROM all_finished_transactions
GROUP BY ALL
ORDER BY
  Project_id,
  profit_percentage DESC