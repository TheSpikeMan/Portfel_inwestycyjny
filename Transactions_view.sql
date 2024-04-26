-- IMPORTING THE DATA INTO TABLES -- 
/*
W tym kroku pobierane są dane:
- transakcyjne, przechowujące dane o transakcjach finansowych,
- walutowe, przechowujące dane o wartości walut USD oraz EUR,
- dane instrumentów finansowych (tickery),
- dane dat wyznaczania wartości walut USD oraz EUR, dla celów wyznaczenia kursu walutowego z poprzedniego dnia roboczego,
- dane kalendarzowe, dla celów podpięcia kwartału do daty transakcji.
*/

WITH 
-- Przechowuje dane transakcji giełdowych
transactions_data AS (SELECT * FROM `projekt-inwestycyjny.Transactions.Transactions`),
-- Przechowuje informacje o kursach walut na dany dzień
currency_data_raw AS (SELECT * FROM `projekt-inwestycyjny.Waluty.Currency`),
-- Przechowuje dane instrumentów finansowych
instruments_data AS (SELECT * FROM `projekt-inwestycyjny.Dane_instrumentow.Instruments`),
-- Przechowuje dane typów instrumentów finansowych
instruments_types AS (SELECT * FROM `projekt-inwestycyjny.Dane_instrumentow.Instrument_types`),
-- Przechowuje pzypisanie dat do kwartałów -- DO ROZWAŻENIA USUNIĘCIE
calendar_dates AS (SELECT * FROM `projekt-inwestycyjny.Calendar.Dates`),
-- Przechowuje dane giełdowe instrumentów finansowych
daily AS (SELECT * FROM `projekt-inwestycyjny.Dane_instrumentow.Daily`),


currency_data AS (
  SELECT
    Currency_date                                   AS Currency_date,
    Currency                                        AS Currency,
    Currency_close                                  AS Currency_close,
    -- Ostatni dzień roboczy
    LAG(Currency_date)  OVER currency_window        AS last_working_day,
    -- Kurs walutowy z ostatniego dnia roboczego
    LAG(Currency_close) OVER currency_window        AS last_currency_close
  FROM currency_data_raw
  QUALIFY TRUE
  WINDOW
    currency_window AS (
      PARTITION BY Currency
      ORDER BY Currency_date
    )
),
-- INITIAL AGGREGATION --
/*
W kroku tym pobierane są wszystkie dane transakcyjne, a następnie dołączane do nich dane tickerów oraz dane z kwerendy określającej ostatnią datę wyznaczenia kursu walutowego.
*/

initial_aggregation AS (
  SELECT 
    * EXCEPT (Instrument_id, Currency, Instrument_type_id, Ticker, Currency_date, last_currency_close),
    instruments_data.Instrument_id                           AS Instrument_id,                       
    instruments_data.Ticker                                  AS Ticker,
    transactions_data.Currency                               AS Currency,
    instruments_types.Instrument_type_id                     AS Instrument_type_id,
    COALESCE(currency_data.Currency_date, Transaction_date)  AS Currency_date,
    COALESCE(currency_data.last_currency_close, 1)           AS last_currency_close,
    -- Utworzenie kolumny, która przechowuje wartość transakcji w PLN
    ROUND(
      Transaction_amount *
      Transaction_price *
      COALESCE(currency_data.last_currency_close, 1)
      , 2)                                                   AS Transaction_value_pln,
    -- Utworzenie grupy instrumentów, dla rozliczeń podatkowych
    CASE
      WHEN Transaction_type = 'Sell'      THEN 'Sell_amount'
      WHEN Transaction_type = 'Wykup'     THEN 'Sell_amount'
      WHEN Transaction_type = 'Buy'       THEN 'Buy_amount'
      WHEN Transaction_type = 'Dywidenda' THEN 'Div_related_amount'
      WHEN Transaction_type = 'Odsetki'   THEN 'Div_related_amount'
    ELSE NULL
    END                                                      AS Transaction_type_group,
    CASE
      WHEN Transaction_type = 'Sell'      THEN SAFE_NEGATE(Transaction_amount)
      WHEN Transaction_type = 'Wykup'     THEN SAFE_NEGATE(Transaction_amount)
      WHEN Transaction_type = 'Buy'       THEN Transaction_amount
      WHEN Transaction_type = 'Dywidenda' THEN 0
      WHEN Transaction_type = 'Odsetki'   THEN 0
    END                                                      AS Transaction_amount_with_sign,  
    -- Liczba dni, która upłynęła od transakcji do dnia dzisiejszego                          
    DATE_DIFF(CURRENT_DATE(), Transaction_date, DAY)         AS age_of_instrument
  FROM transactions_data
  -- Połączenie z danymi instrumentów
  LEFT JOIN instruments_data
  ON transactions_data.Instrument_id = instruments_data.Instrument_id
  -- Połączenie z danymi typów instrumentów
  LEFT JOIN instruments_types
  ON instruments_data.Instrument_type_id = instruments_types.Instrument_type_id
  -- Połączenie z danymi giełdowymi
  LEFT JOIN daily
  ON instruments_data.Ticker = daily.Ticker
  AND transactions_data.Transaction_date = daily.Date
  -- Połączanie z danymi walutowymi
  LEFT JOIN currency_data
  ON transactions_data.Transaction_date  = currency_data.Currency_date
  AND transactions_data.Currency          = currency_data.Currency
),

-- PRE FINAL AGGREGATION --
/*
W tym kroku wyznaczona jest wartość instrumentu oraz jego wolumen, in total, na moment transakcji.
Dodatkowo wyznaczona jest łączna wartość sprzedaży - wyświetlana wyłącznie dla typu 'Sell' oraz łączna wartość zakupów po typie 'Buy'.
Oznaczenie kolumn:
- transaction_date_buy_ticker_amount - suma wolumenu zakupowego lub sprzedażowego na moment transakcji - wartość jest sumowana po typie transakcji (buy/sell)
- transaction_date_ticker_amount - suma wolumenu uwzględniająca rodzaj transakcji - jest to aktualna ilość wolumenu danego instrumentu na moment transakcji
- cumulative_sell_amoutn_per_ticker - jest to łączna, niezależna od daty transakcji wartość wolumenu sprzedanego danego instrumentu
*/


-- DO POPRAWKI OD TEGO MIEJSCA

pre_final_aggregation AS (
  SELECT
    *,
    SUM(Transaction_amount_with_sign) OVER transaction_amount_until_transaction_date           AS transaction_date_ticker_amount,
    SUM(Transaction_amount)           OVER transaction_amount_with_type_until_transaction_date AS transaction_date_buy_ticker_amount,
    CASE 
      WHEN Transaction_type_group = 'Sell_amount' THEN SUM(Transaction_amount) OVER transaction_sell_amount_window
    ELSE NULL
    END AS cumulative_sell_amount_per_ticker
  FROM
    initial_aggregation
  WINDOW
    transaction_amount_until_transaction_date AS (
            PARTITION BY Ticker 
            ORDER BY Transaction_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ),

    transaction_amount_with_type_until_transaction_date AS (
            PARTITION BY Ticker, Transaction_type_group 
            ORDER BY Transaction_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ),

    transaction_sell_amount_window AS (
      PARTITION BY Ticker, Transaction_type_group 
      ORDER BY Transaction_date 
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )
),
 
almost_final_aggregation AS (
  SELECT
    *,
    CASE
      WHEN Transaction_type_group <> "Div_related_amount" THEN ROUND(SUM(transaction_date_ticker_amount) OVER(PARTITION BY Instrument_type_id ORDER BY Transaction_date ROWS BETWEEN CURRENT ROW AND CURRENT ROW) * Close, 2) 
    ELSE 0
    END AS instrument_type_cumulative_value,
    ROUND(transaction_date_ticker_amount * Close, 2) AS transaction_date_ticker_value
  FROM 
    pre_final_aggregation
),

-- FINAL AGGREGATION --
/*
W kroku tym następuje przypianie do okna dla danego tickera, w zakresie kolumny cumulative_sell_amount_per_ticker wartości dla każdego typu danych (wcześniej była ona podpięta tylko do wartości sprzedaży) - tym samym cała kolumna dla danego tickera prezentuje skumulowaną wartość jego sprzedaży.
Jeżeli nie ma żadnych sprzedaży wartość tego parametru przyjmie 0.
*/

final_aggregation AS (
  SELECT
    * EXCEPT(cumulative_sell_amount_per_ticker),
    IFNULL(MAX(cumulative_sell_amount_per_ticker) OVER (PARTITION BY Ticker), 0) AS cumulative_sell_amount_per_ticker,
  FROM
    almost_final_aggregation
)

SELECT * FROM final_aggregation ORDER BY Transaction_date DESC;

