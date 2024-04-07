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
transactions_data AS (SELECT * FROM `projekt-inwestycyjny.Transactions.Transactions`),
currency_data AS (SELECT * FROM `projekt-inwestycyjny.Waluty.Currency`),
tickers_data AS (SELECT * FROM `projekt-inwestycyjny.Dane_instrumentow.Instruments`),
instruments_types AS (SELECT * FROM `projekt-inwestycyjny.Dane_instrumentow.Instrument_types`),
unique_dates AS (SELECT DISTINCT Currency_date FROM currency_data),
calendar_dates AS (SELECT * FROM `projekt-inwestycyjny.Calendar.Dates`),
daily AS (SELECT * FROM `projekt-inwestycyjny.Dane_instrumentow.Daily`),

-- DATES AGGREAGTION --
/*
Widok stworzony na potrzeby wyznaczania ostatniego dnia roboczego danej transakcji.
Do daty transakcji przypisywany jest ostatni dzień wyznaczenia kursu walutowego wobec danej dany, za pomocą funkcji LAG.
*/

dates_list AS (
  SELECT DISTINCT 
    Currency_date as Currency_date,
    LAG(Currency_date) OVER(ORDER BY Currency_date) as last_working_day
  FROM
    unique_dates
  ORDER BY Currency_date
),

-- DAILY DATA --
/*
Widok stworzony w celu pobraniu danych kursów instrumentów na dany moment. 
*/

daily_data AS (
  SELECT *
  FROM daily
),

-- INITIAL AGGREGATION --
/*
W kroku tym pobierane są wszystkie dane transakcyjne, a następnie dołączane do nich dane tickerów oraz dane z kwerendy określającej ostatnią datę wyznaczenia kursu walutowego.
*/

initial_aggregation AS (
  SELECT 
    * EXCEPT (Instrument_id, Currency, Instrument_type_id, Ticker),
    tickers_data.Instrument_id,
    tickers_data.Ticker,
    transactions_data.Currency,
    instruments_types.Instrument_type_id
  FROM transactions_data
  LEFT JOIN tickers_data
  ON transactions_data.Instrument_id = tickers_data.Instrument_id
  LEFT JOIN instruments_types
  ON tickers_data.Instrument_type_id = instruments_types.Instrument_type_id
  LEFT JOIN dates_list
  ON transactions_data.Transaction_date = dates_list.Currency_date
  LEFT JOIN daily
  ON tickers_data.Ticker = daily.Ticker
  AND transactions_data.Transaction_date = daily.Date
),

-- DATA MID AGGREGATION --
/*
W kolejnym kroku wyciągane są wszystkie dane z poprzedniego widoku oraz dokładane:
- dane walutowe na podstawie połączenia ostatniego dnia roboczego z poprzedniego kroku oraz dnia wyznaczenia wartości waluty,
- dane kalendarzowe w celu podpięcia odpowiedniego kwartał pod dane.
Wyznaczana jest dodatkowo wartość transakcji w jednostkch PLN.
*/

data_mid_aggregation AS (
  SELECT
    * EXCEPT (Currency_date, Currency, Currency_close, Date),
    IFNULL(currency_data.Currency_date, transaction_date) as Currency_data,
    initial_aggregation.Currency as Currency,
    IFNULL(currency_data.Currency_close, 1) as Currency_close,
    CASE
      WHEN initial_aggregation.Currency = 'PLN' THEN ROUND(Transaction_amount * Transaction_price, 2)
    ELSE ROUND (Transaction_amount * Transaction_price * Currency_close, 2)
    END AS Transaction_value_pln,
    CASE
      WHEN Transaction_type = 'Sell' THEN 'Sell_amount'
      WHEN Transaction_type = 'Wykup' THEN 'Sell_amount'
      WHEN Transaction_type = 'Buy' THEN 'Buy_amount'
      WHEN Transaction_type = 'Dywidenda' THEN 'Div_related_amount'
      WHEN Transaction_type = 'Odsetki' THEN 'Div_related_amount'
    ELSE NULL
    END AS Transaction_type_group
  FROM initial_aggregation
  LEFT JOIN currency_data
  ON currency_data.Currency_date = initial_aggregation.last_working_day
  AND currency_data.Currency = initial_aggregation.Currency 
  LEFT JOIN calendar_dates
  ON calendar_dates.Date = initial_aggregation.Transaction_date
  ORDER BY initial_aggregation.Transaction_id DESC
),

-- INTERMEDIATE AGGREGATION --
/*
W kroku tym dokonywana jest przypisanie do wolumentu oraz sprzedaży wartości znaku:
- Dla wolumenu i wartości zakupowych przyjęty jest znak "+"
- Dla wolumenu i wartośći sprzedażowych (oraz wykupu) przyjęty jest znak "-"

Dodatkowo obliczany jest wiek instrumentu w postaci zmiennej age_of_instrument
*/

intermediate_aggregation AS (
  SELECT
    *,
    CASE
      WHEN Transaction_type_group = 'Buy_amount' THEN Transaction_amount
      WHEN Transaction_type_group = 'Sell_amount' THEN (-1) * Transaction_amount
      WHEN Transaction_type_group = 'Div_related_amount' THEN 0
    ELSE 0
    END AS Transaction_amount_with_sign,
    DATE_DIFF(CURRENT_DATE(), Transaction_date, DAY) AS age_of_instrument
  FROM data_mid_aggregation
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

pre_final_aggregation AS (
  SELECT
    *,
    SUM(Transaction_amount_with_sign) OVER transaction_amount_until_transaction_date AS transaction_date_ticker_amount,
    SUM(Transaction_amount)           OVER transaction_amount_with_type_until_transaction_date AS transaction_date_buy_ticker_amount,
    CASE WHEN Transaction_type_group = 'Sell_amount' THEN SUM(Transaction_amount) OVER transaction_sell_amount_window
    ELSE NULL
    END AS cumulative_sell_amount_per_ticker
  FROM
    intermediate_aggregation
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

