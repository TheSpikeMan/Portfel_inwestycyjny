WITH
transactions_view_raw          AS (SELECT * FROM `projekt-inwestycyjny.Transactions.Transactions_view`),
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

calendar AS (
  SELECT
    `date` AS calendar_date,
  FROM calendar_raw
  WHERE TRUE
    AND `date` <= CURRENT_DATE('Europe/Warsaw')
),

transactions_view AS (
  SELECT
    project_id,
    instrument_id,
    ticker,
    transaction_timestamp,
    transaction_date_ticker_amount
  FROM transactions_view_raw
  WHERE TRUE
    AND transaction_type NOT IN (
      "Dywidenda",
      "Odsetki"
    )
  QUALIFY TRUE
    AND ROW_NUMBER() OVER last_transaction_per_project_and_ticker_and_day = 1
  WINDOW
    last_transaction_per_project_and_ticker_and_day AS (
      PARTITION BY
        project_id,
        instrument_id,
        ticker,
        transaction_timestamp
      ORDER BY
        transaction_timestamp DESC

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

-- Joining data together --

ticker_date_amount_value AS (
  SELECT
    c.calendar_date,
    i.project_id,
    i.ticker,
    i.instrument_id,
    i.instrument_type_id,
    LAST_VALUE(tv.transaction_date_ticker_amount IGNORE NULLS) OVER (
        PARTITION BY i.project_id, i.instrument_id
        ORDER BY c.calendar_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS amount,
    LAST_VALUE(d.close IGNORE NULLS)
    OVER (
      PARTITION BY i.ticker
      ORDER BY c.calendar_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS price
  FROM calendar AS c
  CROSS JOIN instruments AS i
  LEFT JOIN transactions_view AS tv
    ON i.project_id = tv.project_id
    AND i.instrument_id = tv.instrument_id
    AND c.calendar_date = CAST(tv.transaction_timestamp AS DATE)
  LEFT JOIN daily AS d
    ON c.calendar_date = d.calendar_date
    AND i.ticker = d.ticker
)


SELECT
  calendar_date,
  project_id,
  ticker,
  instrument_id,
  instrument_type_id,
  amount,
  price,
  IFNULL(amount * price, 0) AS daily_value
FROM ticker_date_amount_value
WHERE TRUE
ORDER BY
  calendar_date DESC