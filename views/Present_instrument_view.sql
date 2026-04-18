WITH
transactions_view_dev_raw AS (SELECT * FROM `projekt-inwestycyjny.Transactions.Transactions_view_DEV`),
daily_raw                 AS (SELECT * FROM `projekt-inwestycyjny.Dane_instrumentow.Daily`),

--- FILTERING DATA ---

transactions_view AS (
  SELECT
    -- Transaction information --
    --transaction_timestamp,
    SAFE_CAST(transaction_timestamp AS DATE) AS transaction_date,
    transaction_id,
    transaction_type,
    project_id,
    currency,
    transaction_amount,
    transaction_date_ticker_amount,
    transaction_price,
    -- Instrument information --
    instrument_id,
    ticker,
    --age_of_instrument,
    -- Instrument type information
    instrument_type_id
  FROM transactions_view_dev_raw
  WHERE TRUE
    AND Transaction_type_group IN (
      "Buy_amount",
      "Sell_amount"
    )
  QUALIFY TRUE
    AND ROW_NUMBER() OVER (
      PARTITION BY Project_id, Ticker
      ORDER BY Transaction_timestamp DESC
    ) = 1
),

transactions_view_max_instrument_age AS (
  SELECT DISTINCT
    project_id,
    instrument_id,
    MAX(age_of_instrument) AS age_of_instrument
  FROM transactions_view_dev_raw
  GROUP BY
    project_id,
    instrument_id
),

daily AS (
  SELECT
    `date` AS calendar_date,
    project_id,
    ticker,
    close
  FROM daily_raw
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY
      project_id,
      ticker
    ORDER BY
      `date` DESC
  ) = 1
),

--- PRESENT INSTRUMENTS BASED ON TRANSACTIONS ---

transaction_view_present_instruments AS (
  SELECT *
  FROM transactions_view
  WHERE TRUE
    AND transaction_date_ticker_amount <> 0
),

--- CALCULATING REMAINING AMOUNT AND AVERAGE PRICE ---
transaction_view_remaining_amount AS (
  SELECT
    --Transaction_timestamp,
    project_id,
    instrument_id,
    transaction_price,
    last_currency_close,
   -- age_of_instrument,
    GREATEST(
      0,
      LEAST(
        transaction_amount_with_sign,
        transaction_date_buy_ticker_amount - COALESCE(cumulative_sell_amount_per_ticker, 0)
      )
    ) AS remaining_amount
  FROM transactions_view_dev_raw
  WHERE TRUE
    AND transaction_type_group = "Buy_amount"
),

transaction_view_average_price AS (
  SELECT
    --MAX(Transaction_timestamp) AS Transaction_timestamp,
    project_id,
    instrument_id,
    SUM(remaining_amount)  AS current_volume,
    -- Average price weighted by wolume --
    SAFE_DIVIDE(
      SUM(remaining_amount * transaction_price * last_currency_close),
      SUM(remaining_amount)
    )                                      AS avg_buy_price,
    -- Total transaction cost --
    SUM(remaining_amount * transaction_price * last_currency_close) AS total_cost
  FROM transaction_view_remaining_amount
  GROUP BY
    project_id,
    instrument_id
  HAVING current_volume > 0
)

--- JOINING TOGETHER TRANSACTIONS AND DAILY DATA ---

SELECT
  tvpi.project_id,
  tvpi.ticker,
  tvpi.instrument_id,
  tvpi.transaction_date_ticker_amount AS current_amount,
  tvap.avg_buy_price                  AS transaction_avg_price,
  tvap.total_cost                     AS transaction_total_cost,
  d.close                             AS current_price,
  SAFE_MULTIPLY(
    d.close,
    tvpi.transaction_date_ticker_amount
  )                                   AS current_value,
  /*
  LAST_VALUE(tvpi.age_of_instrument) OVER (
    PARTITION BY
      tvap.project_id,
      tvap.instrument_id
    ORDER BY
      tvap.transaction_timestamp DESC
  )                                   AS age_of_instrument
  */
FROM transaction_view_present_instruments AS tvpi
LEFT JOIN daily         AS d
  ON tvpi.ticker = d.ticker
LEFT JOIN transaction_view_average_price AS tvap
  ON tvap.project_id = tvpi.project_id
  AND tvap.instrument_id = tvpi.instrument_id
WHERE TRUE
  AND tvpi.Project_id = 1
ORDER BY
  Ticker