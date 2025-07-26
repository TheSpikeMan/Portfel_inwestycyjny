WITH
transaction_view_raw           AS (SELECT * FROM `projekt-inwestycyjny.Transactions.Transactions_view`),
instrument_types               AS (SELECT * FROM `projekt-inwestycyjny.Dane_instrumentow.Instrument_types`),
instruments                    AS (SELECT DISTINCT Project_id, Ticker FROM `projekt-inwestycyjny.Dane_instrumentow.Instruments`),
daily_raw                      AS (SELECT * FROM `projekt-inwestycyjny.Dane_instrumentow.Daily`),
calendar_raw                   AS (SELECT * FROM `projekt-inwestycyjny.Calendar.Dates`),

-- FILTROWANIE DANYCH

calendar AS (
  SELECT
    `date` AS date
  FROM calendar_raw
  WHERE TRUE
    AND `date` <= CURRENT_DATE()
),

calendar_instruments           AS (
  SELECT * 
  FROM calendar 
  CROSS JOIN instruments),

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

transaction_view                AS (
  SELECT *
  FROM transaction_view_raw
  WHERE TRUE
    AND Transaction_type <> "Dywidenda"
  QUALIFY TRUE
    AND ROW_NUMBER() OVER last_transaction_per_ticker = 1
  WINDOW
    last_transaction_per_ticker AS (
      PARTITION BY
        Ticker,
        Transaction_date
      ORDER BY
        Transaction_id DESC
    )
),

daily AS (
  SELECT *
  FROM daily_raw
  QUALIFY TRUE AND ROW_NUMBER() OVER unique_entries = 1
  WINDOW
    unique_entries AS (
      PARTITION BY
        `Date`,
        Ticker
    )
),

ticker_date_amount_value AS (
SELECT
  calendar_instruments.Project_id                                                              AS Project_id,
  calendar_instruments.date                                                                    AS `Date`,
  calendar_instruments.Ticker                                                                  AS Ticker,
  COALESCE(instrument_types.Instrument_type, 
    LAST_VALUE(instrument_types.Instrument_type IGNORE NULLS) OVER window_transactions_by_ticker)      AS Instrument_type,
  COALESCE(transaction_date_ticker_amount,
    LAST_VALUE(transaction_date_ticker_amount IGNORE NULLS) OVER window_transactions_by_ticker)        AS transaction_date_ticker_amount,
  ROUND(COALESCE(daily.Close, 
    LAST_VALUE(daily.Close IGNORE NULLS) OVER window_transactions_by_ticker), 2)                       AS Close,
  ROUND(COALESCE(transaction_date_ticker_amount,
    LAST_VALUE(transaction_date_ticker_amount IGNORE NULLS) OVER window_transactions_by_ticker) * 
    COALESCE(daily.Close, LAST_VALUE(daily.Close IGNORE NULLS) OVER window_transactions_by_ticker), 2) AS ticker_date_value
FROM calendar_instruments
LEFT JOIN transaction_view
  ON calendar_instruments.date = transaction_view.Transaction_date
  AND calendar_instruments.Ticker = transaction_view.Ticker
  AND calendar_instruments.Project_id = transaction_view.Project_id
LEFT JOIN daily
  ON calendar_instruments.date = daily.`Date`
  AND calendar_instruments.Ticker = daily.Ticker
LEFT JOIN instrument_types
  ON transaction_view.Instrument_type_id = instrument_types.Instrument_type_id
WHERE TRUE
QUALIFY TRUE
WINDOW
  window_transactions_by_ticker AS (
    PARTITION BY 
      calendar_instruments.Project_id, 
      calendar_instruments.Ticker 
    ORDER BY calendar_instruments.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),

share_of_portfolio_included AS (
  SELECT
    *,
    ROUND(100 * ticker_date_value/SUM(ticker_date_value) OVER date_window, 2) AS share_of_portfolio
  FROM ticker_date_amount_value
  WINDOW
    date_window AS (
      PARTITION BY 
        Project_id,
        `Date`
    )
  ORDER BY
    `Date` DESC
)

SELECT * 
FROM share_of_portfolio_included
WHERE TRUE