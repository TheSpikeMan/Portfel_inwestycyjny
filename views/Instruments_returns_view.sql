--- Fetching data ---

WITH
daily_raw                  AS (SELECT * FROM `projekt-inwestycyjny.Dane_instrumentow.Daily`),
calendar_raw               AS (SELECT * FROM `projekt-inwestycyjny.Calendar.Dates`),
instruments_raw            AS (SELECT * FROM `projekt-inwestycyjny.Dane_instrumentow.Instruments`),
transactions_view_raw      AS (SELECT * FROM `projekt-inwestycyjny.Transactions.Transactions_view`),

--- Filtering data ---

daily AS (
  SELECT DISTINCT
    `date` AS calendar_date,
    project_id,
    ticker,
    close
  FROM Daily_raw
),

calendar AS (
  SELECT DISTINCT
    `date` AS calendar_date
  FROM Calendar_raw
),

instruments AS (
  SELECT DISTINCT
    project_id,
    instrument_id,
    ticker,
    instrument_type_id
  FROM Instruments_raw
),

transactions AS (
  SELECT DISTINCT
    project_id,
    instrument_id,
    CAST(transaction_timestamp AS DATE) AS transaction_date,
    SUM(transaction_amount_with_sign)   AS daily_net_amount,
    SUM(
      CASE
        WHEN transaction_type = "Buy"   THEN transaction_value_pln
        WHEN transaction_type = "Sell"
          OR transaction_type = "Wykup" THEN (-1) * transaction_value_pln
      ELSE 0
      END
     ) AS daily_net_cashflow
  FROM transactions_view_raw
  GROUP BY 1,2,3
),


--- Joining together sources ---

cleaned_price_history AS (
  SELECT
    c.calendar_date,
    i.project_id,
    i.instrument_id,
    i.ticker,
    i.instrument_type_id,
    COALESCE(
      d.close,
      LAST_VALUE(d.close IGNORE NULLS) OVER (
        PARTITION BY i.ticker
        ORDER BY c.calendar_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      )
    )                               AS adjusted_close
  FROM calendar                     AS c
  CROSS JOIN instruments            AS i
  LEFT JOIN daily                   AS d
    ON c.calendar_date = d.calendar_date
    AND d.ticker = i.ticker
  WHERE TRUE
    AND c.calendar_date <= CURRENT_DATE('Europe/Warsaw') - 1
),

--- Joining information about transactions  ---

daily_holdings AS (
  SELECT
    c.*,
    t.transaction_date,
    t.daily_net_amount,
    t.daily_net_cashflow,
    SUM(t.daily_net_amount) OVER w_project_ticker_order_by_date AS daily_transaction_amount_by_transactions
  FROM cleaned_price_history AS c
  LEFT JOIN transactions AS t
    ON c.calendar_date = t.transaction_date
    AND c.instrument_id = t.instrument_id
  WHERE TRUE
    --- Excluding treasure and corporate bonds without daily data
    AND
    (
        (c.Instrument_type_id IN (5,7)
        AND c.adjusted_close IS NOT NULL)
        OR
        --- Or all other type data
        (c.Instrument_type_id IN (1, 2, 3, 4, 6))
    )
  WINDOW
    w_project_ticker_order_by_date AS (
      PARTITION BY
        c.Project_id,
        c.Instrument_id
      ORDER BY
        c.calendar_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )
),

--- Calculating daily market value and daily cashflow ---

daily_holdings_extended AS (
  SELECT
    d.*,
    adjusted_close *
    COALESCE(
      daily_transaction_amount_by_transactions,
      LAST_VALUE(daily_transaction_amount_by_transactions IGNORE NULLS) OVER(PARTITION BY Project_id, Instrument_id ORDER BY calendar_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
      0
    )                       AS daily_end_market_value,
    COALESCE(
      daily_net_cashflow,
      0
    )                       AS daily_end_cashflow
  FROM daily_holdings AS d
  WHERE TRUE
    --AND d.daily_transaction_amount_by_transactions > 0
    --OR d.daily_net_cashflow > 0
),

daily_market_value_yesterday AS (
  SELECT
    d.*,
    LAG(daily_end_market_value) OVER (PARTITION BY Project_id, Instrument_id ORDER BY calendar_date) AS daily_begin_market_value
  FROM daily_holdings_extended AS d
),

instrument_epoch AS (
  SELECT
    d.*,
    SUM(
      CASE
        WHEN daily_begin_market_value = 0
          AND daily_end_market_value > 0
          THEN 1
        ELSE 0 END)
      OVER (PARTITION BY d.project_id, d.instrument_id ORDER BY d.calendar_date) AS epoch_id
  FROM daily_market_value_yesterday AS d
),

--- LEVEL 1: Basic single instrument level  ---
base_instrument_level AS (
  SELECT
    'Instrument'                          AS aggregation_level,
    epoch_id,
    calendar_date,
    project_id,
    instrument_id,
    ticker,
    instrument_type_id,
    adjusted_close,
    daily_transaction_amount_by_transactions,
    COALESCE(daily_begin_market_value, 0) AS daily_begin_market_value,
    daily_end_market_value,
    daily_end_cashflow
  FROM instrument_epoch
),

--- LEVEL 2: Instrument type level ---
type_aggregation AS (
  SELECT
    'Instrument_type'             AS aggregation_level,
    epoch_id,
    calendar_date,
    project_id,
    NULL                          AS instrument_id,
    CAST(NULL AS STRING)          AS ticker,
    instrument_type_id,
    NULL                          AS adjusted_close,
    NULL                          AS daily_transaction_amount_by_transactions,
    SUM(daily_begin_market_value) AS daily_begin_market_value,
    SUM(daily_end_market_value)   AS daily_end_market_value,
    SUM(daily_end_cashflow)       AS daily_end_cashflow
  FROM base_instrument_level
  GROUP BY epoch_id, calendar_date, project_id, instrument_type_id
),

--- LEVEL 3: Project level ---
project_aggregation AS (
  SELECT
    'PROJECT'                     AS aggregation_level,
    epoch_id,
    calendar_date,
    project_id,
    NULL                          AS instrument_id,
    NULL                          AS ticker,
    NULL                          AS instrument_type_id,
    NULL                          AS adjusted_close,
    NULL                          AS daily_transaction_amount_by_transactions,
    SUM(daily_begin_market_value) AS daily_begin_market_value,
    SUM(daily_end_market_value)   AS daily_end_market_value,
    SUM(daily_end_cashflow)       AS daily_end_cashflow
  FROM base_instrument_level
  GROUP BY calendar_date, project_id, epoch_id
),

--- Combining all level together  ---
combined_levels AS (
  SELECT * FROM base_instrument_level
  UNION ALL
  SELECT * FROM type_aggregation
  UNION ALL
  SELECT * FROM project_aggregation
)

SELECT
  -- Metadata --
  aggregation_level,
  epoch_id,
  calendar_date,
  project_id,
  instrument_id,
  ticker,
  instrument_type_id,
  -- Daily state --
  adjusted_close,
  daily_transaction_amount_by_transactions,
  daily_begin_market_value,
  daily_end_market_value,
  daily_end_cashflow,
  -- Performance --
  CASE
    WHEN daily_begin_market_value <= 0 THEN 1
    WHEN (daily_end_market_value - daily_end_cashflow) <= 0 THEN 0.000001
    ELSE SAFE_DIVIDE(daily_end_market_value - daily_end_cashflow, daily_begin_market_value)
  END AS daily_hpr
FROM combined_levels