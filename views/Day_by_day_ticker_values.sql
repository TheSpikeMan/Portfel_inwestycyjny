WITH
transaction_view AS (SELECT * FROM `projekt-inwestycyjny.Transactions.Transactions_view` WHERE Transaction_type <> "Dywidenda"),
instrument_types AS (SELECT * FROM `projekt-inwestycyjny.Dane_instrumentow.Instrument_types`),
present_instruments AS (SELECT DISTINCT Ticker FROM `projekt-inwestycyjny.Transactions.Present_transactions_view`),
daily AS (SELECT * FROM `projekt-inwestycyjny.Dane_instrumentow.Daily`),
calendar AS (SELECT dates FROM UNNEST(GENERATE_DATE_ARRAY('2020-01-01', CURRENT_DATE(), INTERVAL 1 DAY)) AS dates),
calendar_present_instruments AS (SELECT * FROM calendar CROSS JOIN present_instruments),

-- TICKER DATE AMOUNT VALUE --
/*
W zapytaniu realizowane są następujące obliczenia:
- wyznaczenie wszystkich dat od '2020-01-01' do daty biężacej,
- przypisanie wszystkich danych transakcyjnych do dat z kalendarza z powyższego okresu,
- dla wszystkich transakcji przypisanie aktualnej ilości posiadanych instrumentów wg obecnego schematu:
  - dla wszystkich dat, dla których realizowana była sprzedaż, przypisanie ilości wg transakcji,
  - dla wszystkich dat, dla których nie była realizowana sprzedaż, przypisanie ilości dla ostatniej dostępnej transakcji (użycie funkcji LAST_VALUE oraz okna), dzięki czemu udaje się zapełnić wszystkie NULLE,
Podobny mechanizm zastosowany jest do wyciągnięcia ceny zamknięcia.
- jako podsumowanie wyznaczana jest wartość instrumentu na każdy kolejny dzień
*/

ticker_date_amount_value AS (
SELECT
  dates                                                                                                AS `Date`,
  calendar_present_instruments.Ticker                                                                  AS Ticker,
  COALESCE(instrument_types.Instrument_type, 
    LAST_VALUE(instrument_types.Instrument_type IGNORE NULLS) OVER window_transactions_by_ticker)      AS Instrument_type,
  COALESCE(transaction_date_ticker_amount,
    LAST_VALUE(transaction_date_ticker_amount IGNORE NULLS) OVER window_transactions_by_ticker)        AS transaction_date_ticker_amount,
  ROUND(COALESCE(daily.Close, 
    LAST_VALUE(daily.Close IGNORE NULLS) OVER window_transactions_by_ticker), 2)                       AS Close,
  ROUND(COALESCE(transaction_date_ticker_amount,
    LAST_VALUE(transaction_date_ticker_amount IGNORE NULLS) OVER window_transactions_by_ticker) * 
    COALESCE(daily.Close, LAST_VALUE(daily.Close IGNORE NULLS) OVER window_transactions_by_ticker), 2) AS ticker_date_value
FROM calendar_present_instruments
LEFT JOIN transaction_view
  ON calendar_present_instruments.dates = transaction_view.Transaction_date
  AND calendar_present_instruments.Ticker = transaction_view.Ticker
LEFT JOIN daily
  ON calendar_present_instruments.dates = daily.`Date`
  AND calendar_present_instruments.Ticker = daily.Ticker
LEFT JOIN instrument_types
  ON transaction_view.Instrument_type_id = instrument_types.Instrument_type_id
QUALIFY TRUE
WINDOW
  window_transactions_by_ticker AS (PARTITION BY calendar_present_instruments.Ticker ORDER BY dates ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),

share_of_portfolio_included AS (
  SELECT
    *,
    ROUND(100 * ticker_date_value/SUM(ticker_date_value) OVER date_window, 2) AS share_of_portfolio
  FROM ticker_date_amount_value
  WINDOW
    date_window AS (
      PARTITION BY `Date`
    )
  ORDER BY
    `Date` DESC
)

SELECT * FROM share_of_portfolio_included;