WITH
transaction_view AS (SELECT * FROM `projekt-inwestycyjny.Transactions.Transactions_view` WHERE Transaction_type <> "Dywidenda"),
present_instruments AS (SELECT DISTINCT Ticker FROM `projekt-inwestycyjny.Transactions.Present_transactions_view`),
daily AS (SELECT * FROM `projekt-inwestycyjny.Dane_instrumentow.Daily`),
calendar AS (SELECT dates FROM UNNEST(GENERATE_DATE_ARRAY('2020-01-01', CURRENT_DATE(), INTERVAL 1 DAY)) AS dates),
calendar_present_instruments AS (SELECT * FROM calendar CROSS JOIN present_instruments),

ticker_date_amount_value AS (
SELECT
  dates,
  calendar_present_instruments.Ticker,
  COALESCE(transaction_view.Instrument_type_id, LAST_VALUE(Instrument_type_id IGNORE NULLS) OVER(PARTITION BY calendar_present_instruments.Ticker ORDER BY dates ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS instrument_type_id,
  COALESCE(transaction_date_ticker_amount,LAST_VALUE(transaction_date_ticker_amount IGNORE NULLS) OVER(PARTITION BY calendar_present_instruments.Ticker ORDER BY dates ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS transaction_date_ticker_amount,
  COALESCE(Close, LAST_VALUE(Close IGNORE NULLS) OVER(PARTITION BY calendar_present_instruments.Ticker ORDER BY dates ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS Close,
  ROUND(COALESCE(transaction_date_ticker_amount,LAST_VALUE(transaction_date_ticker_amount IGNORE NULLS) OVER(PARTITION BY calendar_present_instruments.Ticker ORDER BY dates ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) * COALESCE(Close, LAST_VALUE(Close IGNORE NULLS) OVER(PARTITION BY calendar_present_instruments.Ticker ORDER BY dates ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)), 2) as ticker_date_value
FROM calendar_present_instruments
LEFT JOIN transaction_view
  ON calendar_present_instruments.dates = transaction_view.Transaction_date
  AND calendar_present_instruments.Ticker = transaction_view.Ticker
LEFT JOIN daily
  ON calendar_present_instruments.dates = daily.`Date`
  AND calendar_present_instruments.Ticker = daily.Ticker
ORDER BY
  dates DESC
),

final_view AS (
  SELECT 
    dates AS `Date`,
    Ticker AS Ticker,
    instrument_type_id AS Instrument_type_id,
    ticker_date_value AS ticker_date_value
  FROM 
    ticker_date_amount_value
)

SELECT *
FROM final_view;