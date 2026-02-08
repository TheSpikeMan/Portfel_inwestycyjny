--- FETCHING DATA

WITH

Daily_raw                  AS (SELECT * FROM `projekt-inwestycyjny.Dane_instrumentow.Daily`),
Calendar_raw               AS (SELECT * FROM `projekt-inwestycyjny.Calendar.Dates`),
Instrument_types_raw       AS (SELECT * FROM `projekt-inwestycyjny.Dane_instrumentow.Instrument_types`),
Instruments_raw            AS (SELECT * FROM `projekt-inwestycyjny.Dane_instrumentow.Instruments`),
Transactions_view_raw      AS (SELECT * FROM `projekt-inwestycyjny.Transactions.Transactions_view`),

--- FILTERING DATA ---

Daily AS (
  SELECT DISTINCT
    Date,
    Project_id,
    Ticker,
    Close
  FROM Daily_raw
),

Calendar AS (
  SELECT DISTINCT
    date,
    year,
    month,
    day,
    quarter,
    quarter_text,
    year_quarter
  FROM Calendar_raw
),

Instruments AS (
  SELECT DISTINCT
    project_id,
    instrument_id,
    ticker,
    market_currency,
    ticker_currency,
    instrument_type_id
  FROM Instruments_raw
),

Transactions AS (
  SELECT DISTINCT
    Project_id,
    Instrument_id,
    Transaction_date,
    Transaction_price,
    Transaction_amount,
    Transaction_amount_with_sign,
    CASE
      WHEN Transaction_type = "Buy"
      THEN Transaction_value_pln
      WHEN Transaction_type = "Sell"
      THEN (-1) * Transaction_value_pln
    ELSE 0
    END AS Transaction_value_pln_with_sign
  FROM Transactions_view_raw AS t
),


--- JOINING TOGETHER SOURCES

Cleaned_price_history AS (
  SELECT
    c.date                          AS date,
    c.year                          AS year,
    c.month                         AS month,
    c.day                           AS day,
    c.quarter                       AS quarter,
    c.quarter_text                  AS quarter_text,
    c.year_quarter                  AS year_quarter,
    i.project_id                    AS project_id,
    i.instrument_id                 AS instrument_id,
    i.ticker                        AS ticker,
    it.instrument_type_id           AS instrument_type_id,
    it.instrument_type              AS instrument_type,
    it.instrument_type_main         AS instrument_type_main,
    COALESCE(
      d.close,
      -- IF NULL THEN AVERAGE OF 2 LAST AVAILABLE
      SAFE_DIVIDE(
        -- FETCHING LAST NON-NULL FOR SPECIFIC ROW (PRECEDING)
        LAST_VALUE(d.close IGNORE NULLS) OVER (
          PARTITION BY i.ticker
          ORDER BY c.date
          ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) +
        -- FETCHING FIRST NON-NULL VALUE FOR SPECIFING RAW (FOLLOWING)
        FIRST_VALUE(d.close IGNORE NULLS) OVER (
          PARTITION BY i.ticker
          ORDER BY c.date
          ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
        ),
        2),
        -- IF NO PRECREDING VALUE AVAILABLE TAKE THE FIRST FOLLOWING
        FIRST_VALUE(d.close IGNORE NULLS) OVER (
          PARTITION BY i.ticker
          ORDER BY c.date
          ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
        )
    )                               AS adjusted_close
  FROM Calendar                     AS c
  CROSS JOIN Instruments            AS i
  LEFT JOIN Daily                   AS d
    ON c.date = d.date
    AND d.ticker = i.ticker
  LEFT JOIN Instrument_types_raw    AS it
    ON it.instrument_type_id = i.instrument_type_id
  WHERE TRUE
    AND c.date <= CURRENT_DATE('Europe/Warsaw') - 1
),

--- CALCULATING DAILY CLOSE CHANGES ---

Daily_price_changes AS (
  SELECT
    c.*,
    SAFE_DIVIDE(
      adjusted_close - LAG(adjusted_close) OVER w_ticker_ordered_by_date,
      LAG(adjusted_close) OVER w_ticker_ordered_by_date
    )                                 AS price_change_daily_pct
  FROM Cleaned_price_history          AS c
  WHERE TRUE
    AND (
      --- EXCLUDING TREASURY AND CORPORATE BONDS FOR WHICH I DON'T HAVE DATA AS THEIR VALUE RELY ON TRANSACTIONS
      c.Instrument_type_id IN (5,7)
      AND c.adjusted_close IS NOT NULL
      )
      --- OR ALL OTHER TYPES
      OR Instrument_type_id IN (1, 2, 3, 4, 6)
  WINDOW
    w_ticker_ordered_by_date AS (
      PARTITION BY ticker
      ORDER BY date
    )
),

--- JOINNG INFORMATION ABOUT DAILY HOLDINGS ---

Daily_holdings AS (
  SELECT
    d.*,
    t.transaction_date,
    t.transaction_amount,
    t.Transaction_amount_with_sign,
    t.Transaction_value_pln_with_sign,
    SUM(Transaction_amount_with_sign) OVER w_project_ticker_order_by_date AS daily_transaction_amount_by_transactions
  FROM Daily_price_changes AS d
  LEFT JOIN Transactions AS t
    ON d.date = t.transaction_date
    AND d.instrument_id = t.instrument_id
  WHERE TRUE
  GROUP BY ALL
  WINDOW
    w_project_ticker_order_by_date AS (
      PARTITION BY
        d.Project_id,
        d.Instrument_id
      ORDER BY
        d.Date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )
),

Daily_holdings_extended AS (
  SELECT
    d.*,
    adjusted_close *
    COALESCE(
      daily_transaction_amount_by_transactions,
      LAST_VALUE(daily_transaction_amount_by_transactions IGNORE NULLS) OVER(PARTITION BY Project_id, Instrument_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
      0
    )                       AS daily_market_value,
    -- DOCELOWO DO POPRAWKI ABY CASHFLOW OBLICZAĆ NA DANYCH TRANSAKCYJNYCH A NIE GIEŁDOWYCH Z KURSEM ZAMKINIĘCIA
    COALESCE(
      Transaction_value_pln_with_sign,
      0
    )                       AS daily_cashflow
  FROM Daily_holdings AS d
),

Daily_returns AS (
  SELECT
    d.*,
    SAFE_DIVIDE(
      daily_market_value - daily_cashflow,
      LAST_VALUE(daily_market_value) OVER (PARTITION BY Project_id, Instrument_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
    ) - 1                      AS daily_twr
  FROM Daily_holdings_extended AS d
),

Cumulative_returns AS (
  SELECT
    d.*,
    EXP(SUM(LN(1 + COALESCE(daily_twr, 0))) OVER (PARTITION BY Project_id, Instrument_id ORDER BY date)) - 1 AS cumulative_twr
  FROM Daily_returns  AS d
)

SELECT *
FROM Cumulative_returns