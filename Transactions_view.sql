
/*
W tym kroku pobierane są dane:
- transakcyjne, przechowujące informacje o transakcjach finansowych,
- walutowe, przechowujące informacje o wartości walut USD oraz EUR na dany dzień względem PLN,
- instrumentów, przechowujące informacje o instrumentach finasowych,
- o typach instrumentów finansowych,
- dziennych kursów giełdowych
*/

WITH 
-- Przechowuje dane transakcji giełdowych
transactions_data AS (SELECT * FROM `projekt-inwestycyjny.Transactions.Transactions`),
-- Przechowuje informacje o kursach walut na dany dzień
currency_data_raw AS (SELECT * FROM `projekt-inwestycyjny.Waluty.Currency_view`),
-- Przechowuje dane instrumentów finansowych
instruments_data  AS (SELECT * FROM `projekt-inwestycyjny.Dane_instrumentow.Instruments`),
-- Przechowuje dane typów instrumentów finansowych
instruments_types AS (SELECT * FROM `projekt-inwestycyjny.Dane_instrumentow.Instrument_types`),
-- Przechowuje dane giełdowe instrumentów finansowych
daily             AS (SELECT * FROM `projekt-inwestycyjny.Dane_instrumentow.Daily`),
-- Przechowuje informacje kalendarzowe
dates             AS (SELECT * FROM `projekt-inwestycyjny.Calendar.Dates`),

-- CURRENCY_DATA --
/* Przechowuje informacje o kursach walutowych, wraz z wyznaczeniem kursu walutowego na dzień poprzedni */
currency_data AS (
  SELECT
    * EXCEPT(Currency_date),
    CAST(Currency_date AS DATE) AS Currency_date
  FROM currency_data_raw
  WHERE TRUE
),

-- DATA AGGREGATED --
/*
Połączenie wszystkich danych.
Wyznaczenie wskaźników:
- Kwartału,
- Wartość transakcji w PLN,
- Grupy transakcyjnej,
- Wolumenu transakcji wraz ze znakiem,
- Liczby dni, która upłynęła od danej transakcji.
*/

data_aggregated AS (
  SELECT 
    * EXCEPT (Instrument_id, Currency, Instrument_type_id, Ticker, Currency_date, last_currency_close, Currency_close),
    instruments_data.Instrument_id                           AS Instrument_id,                       
    instruments_data.Ticker                                  AS Ticker,
    transactions_data.Currency                               AS Currency,
    instruments_types.Instrument_type_id                     AS Instrument_type_id,
    COALESCE(currency_data.Currency_date, Transaction_date)  AS Currency_date,
    COALESCE(Currency_close, 1)                              AS Currency_close,
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
  ON transactions_data.Instrument_id      = instruments_data.Instrument_id
  -- Połączenie z danymi typów instrumentów
  LEFT JOIN instruments_types
  ON instruments_data.Instrument_type_id  = instruments_types.Instrument_type_id
  -- Połączenie z danymi giełdowymi
  LEFT JOIN daily
  ON instruments_data.Ticker = daily.Ticker
  AND transactions_data.Transaction_date  = daily.Date
  -- Połączanie z danymi walutowymi
  LEFT JOIN currency_data
  ON transactions_data.Transaction_date   = currency_data.Currency_date
  AND transactions_data.Currency          = currency_data.Currency
  -- Połączenie z danymi kalendarza
  LEFT JOIN dates
  ON transactions_data.Transaction_date   = dates.Date
),


-- PRE FINAL AGGREGATION --
/*
Oznaczenie kolumn:
- transaction_date_buy_ticker_amount - suma wolumenu zakupowego lub sprzedażowego na moment transakcj. 
Wartość jest sumowana po typie transakcji (buy/sell).
- transaction_date_ticker_value - wartość giełdowa instrumentu finansowego na moment transakcji.
- transaction_date_ticker_amount - suma wolumenu uwzględniająca rodzaj transakcji. 
Jest to aktualna ilość wolumenu danego instrumentu na moment transakcji
- cumulative_sell_amoutn_per_ticker - jest to łączna, niezależna od daty transakcji wartość wolumenu sprzedanego danego instrumentu
*/

data_aggregated_with_windows AS (
  SELECT
    *,
    -- Wyznacza wartość wolumenu danego instrumentu na moment danej transakcji (po danej transakcji)
    SUM(Transaction_amount_with_sign)       OVER transaction_amount_until_transaction_date      AS transaction_date_ticker_amount,
    -- Wyznacza wartość danego instrumentu na moment danej transakcji (po danej transakcji)
    ROUND(SUM(Transaction_amount_with_sign) OVER transaction_amount_until_transaction_date * Close, 2)   
                                                                                                AS transaction_date_ticker_value,
    -- Wyznacza wartość wolumenu danego instrumentu na moment danej transakcji (po danej transakcji), lecz z uwzglęnieniem typu transakcji
    SUM(Transaction_amount)                 OVER transaction_amount_with_type_until_transaction_date 
                                                                                                AS transaction_date_buy_ticker_amount,
    -- Wyznacza całkowitą wartość sprzedaży danego instrumentu finansowego
    CASE 
      WHEN Transaction_type_group = 'Sell_amount' THEN SUM(Transaction_amount) OVER transaction_sell_amount_window
      ELSE NULL
      END                                                                                       AS cumulative_sell_amount_per_ticker,
  FROM
    data_aggregated
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

-- FINAL AGGREGATION --
/*
Oznaczenie kolumn:
- instrument_type_cumulative_value - wartość wszystkich instrumentów danego typu na moment transakcji,
- cumulative_sell_amount_per_ticker - wartość sprzedaży danego instrumentu skumulowana TOTAL
*/


final_aggregation AS (
  SELECT
    * EXCEPT(cumulative_sell_amount_per_ticker),
    -- Określenie wartości danego typu instrumentu na danych dzień
    CASE
      WHEN Transaction_type_group <> "Div_related_amount" 
      THEN ROUND(SUM(transaction_date_ticker_amount) OVER instrument_type_window_until_transaction_day_window * Close, 2)                                                                                         
      ELSE 0
    END                                                                                         AS instrument_type_cumulative_value,
    -- Przepisanie wartości sprzedaży danego instrumentu na wiersze zakupowe & dywidendowe itp.
    COALESCE(
      MAX(cumulative_sell_amount_per_ticker) OVER instrument_window,
      0
    )                                                                                           AS cumulative_sell_amount_per_ticker
  FROM
    data_aggregated_with_windows
  WINDOW
    instrument_type_window_until_transaction_day_window AS (
      PARTITION BY Instrument_type_id
      ORDER BY Transaction_date
      ROWS BETWEEN CURRENT ROW AND CURRENT ROW
    ),
    instrument_window AS (
      PARTITION BY Ticker
    )
)

SELECT
  *
FROM final_aggregation 
ORDER BY Transaction_date 

