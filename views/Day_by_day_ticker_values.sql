WITH
transactions_view_raw          AS (SELECT * FROM `projekt-inwestycyjny.Transactions.Transactions_view`),
instrument_types_raw           AS (SELECT * FROM `projekt-inwestycyjny.Dane_instrumentow.Instrument_types`),
instruments_raw                AS (SELECT * FROM `projekt-inwestycyjny.Dane_instrumentow.Instruments`),
daily_raw                      AS (SELECT * FROM `projekt-inwestycyjny.Dane_instrumentow.Daily`),
calendar_raw                   AS (SELECT * FROM `projekt-inwestycyjny.Calendar.Dates`),

--- Filtering data ---

instruments AS (
  SELECT
    project_id,
    instrument_id,
    ticker,
    instrument_type_id
  FROM instruments_raw
  GROUP BY ALL
),

instrument_types AS (
  SELECT
    instrument_type_id,
    instrument_type,
    instrument_type_main
  FROM instrument_types_raw
),

calendar AS (
  SELECT
    `date` AS calendar_date,
    year,
    month,
    day,
    quarter,
    quarter_text,
    year_quarter,
    week
  FROM calendar_raw
  WHERE TRUE
    AND `date` <= CURRENT_DATE('Europe/Warsaw')
),

transactions_view AS (
  SELECT
    project_id,
    instrument_id,
    ticker,
    transaction_date,
    transaction_date_ticker_amount
  FROM transactions_view_raw
  WHERE TRUE
    AND transaction_type <> "Dywidenda"
  QUALIFY TRUE
    AND ROW_NUMBER() OVER last_transaction_per_project_and_ticker_and_day = 1
  WINDOW
    last_transaction_per_project_and_ticker_and_day AS (
      PARTITION BY
        project_id,
        instrument_id,
        ticker,
        transaction_date
      ORDER BY
        transaction_id DESC
    )
),

daily AS (
  SELECT
    `date` AS calendar_date,
    ticker,
    close
  FROM daily_raw
  QUALIFY TRUE AND ROW_NUMBER() OVER unique_entries = 1
  WINDOW
    unique_entries AS (
      PARTITION BY
        `Date`,
        Ticker
    )
),

--- Joining together calendar with instruments dimension table ---

calendar_with_instruments  AS (
  SELECT
    -- Calendar Data --
    calendar_date,
    year,
    month,
    day,
    quarter,
    quarter_text,
    year_quarter,
    week,
    -- Instruments data --
    project_id,
    ticker,
    instrument_id,
    instrument_type_id
  FROM calendar
  CROSS JOIN instruments
),

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
    cwi.project_id                                                              AS project_id,
    cwi.calendar_date                                                           AS calendar_date,
    cwi.year                                                                    AS year,
    cwi.month                                                                   AS month,
    cwi.quarter                                                                 AS quarter,
    cwi.quarter_text                                                            AS quarter_text,
    cwi.ticker                                                                  AS ticker,
    it.instrument_type_id                                                       AS instrument_type_id,
    it.instrument_type                                                          AS instrument_type,
    it.instrument_type_main                                                     AS instrument_type_main,
    COALESCE(
      transaction_date_ticker_amount,
      LAST_VALUE(transaction_date_ticker_amount IGNORE NULLS)
      OVER window_project_and_ticker_total_until_current_row
    )                                                                           AS ticker_date_amount,
    COALESCE(
      d.close,
      LAST_VALUE(d.close IGNORE NULLS)
      OVER window_project_and_ticker_total_until_current_row
    )                                                                           AS close,
    COALESCE(
      transaction_date_ticker_amount,
      LAST_VALUE(transaction_date_ticker_amount IGNORE NULLS)
      OVER window_project_and_ticker_total_until_current_row
      ) *
    COALESCE(
      d.Close,
      LAST_VALUE(d.Close IGNORE NULLS)
      OVER window_project_and_ticker_total_until_current_row)                   AS value
  FROM calendar_with_instruments AS cwi
  -- Joining with fact tables --
  LEFT JOIN transactions_view     AS tv
    ON cwi.project_id = tv.project_id
    AND cwi.instrument_id = tv.instrument_id
    AND cwi.calendar_date = tv.Transaction_date
  LEFT JOIN daily AS d
    ON cwi.calendar_date = d.calendar_date
    AND cwi.Ticker = d.Ticker
  -- Joining with dimension tables --
  LEFT JOIN instrument_types AS it
    ON cwi.instrument_type_id = it.instrument_type_id
  WINDOW
    window_project_and_ticker_total_until_current_row AS (
      PARTITION BY
        cwi.project_id,
        cwi.instrument_id,
        cwi.ticker
      ORDER BY
        cwi.calendar_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      )
),

instrument_types_wtih_value_sums AS (
  SELECT
    project_id,
    calendar_date,
    year,
    month,
    quarter,
    quarter_text,
    instrument_type,
    ticker_date_amount,
    close,
    value,
    SUM(value) OVER window_project_and_instrument_type_by_day AS daily_total_value
  FROM ticker_date_amount_value
  WINDOW
    window_project_and_instrument_type_by_day AS (
      PARTITION BY
        calendar_date,
        project_id,
        instrument_type
    )
)

SELECT
  project_id,
  calendar_date,
  year,
  month,
  quarter,
  quarter_text,
  instrument_type,
  ticker_date_amount,
  close,
  value,
  LAST_VALUE(daily_total_value) OVER window_project_and_instrument_type_yearly_until_current_row    AS ytd_total_value,
  LAST_VALUE(daily_total_value) OVER window_project_and_instrument_type_monthly_until_current_row   AS mtd_total_value,
  LAST_VALUE(daily_total_value) OVER window_project_and_instrument_type_quarterly_until_current_row AS qtd_total_value,
  SAFE_DIVIDE(
    value,
    SUM(value) OVER date_window
    ) AS share_of_portfolio
FROM instrument_types_wtih_value_sums
WINDOW
  date_window AS (
    PARTITION BY
      Project_id,
      Calendar_date
  ),
  window_project_and_instrument_type_yearly_until_current_row AS (
    PARTITION BY
      project_id,
      year,
      instrument_type
    ORDER BY
      calendar_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  ),
  window_project_and_instrument_type_monthly_until_current_row AS (
    PARTITION BY
      project_id,
      year,
      month,
      instrument_type
    ORDER BY
      calendar_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ),
  window_project_and_instrument_type_quarterly_until_current_row AS (
    PARTITION BY
      project_id,
      year,
      quarter,
      instrument_type
    ORDER BY
      calendar_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  )
ORDER BY
  calendar_date DESC