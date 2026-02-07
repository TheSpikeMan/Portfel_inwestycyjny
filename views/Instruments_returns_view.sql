--- FETCHING DATA

WITH

Daily_raw           AS (SELECT * FROM `projekt-inwestycyjny.Dane_instrumentow.Daily`),
Calendar_raw        AS (SELECT * FROM `projekt-inwestycyjny.Calendar.Dates`),
Instrument_types_raw AS (SELECT * FROM `projekt-inwestycyjny.Dane_instrumentow.Instrument_types`),
Instruments_raw     AS (SELECT * FROM `projekt-inwestycyjny.Dane_instrumentow.Instruments`),
Transactions_raw    AS (SELECT * FROM `projekt-inwestycyjny.Transactions.Transactions`),

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
)

SELECT *
FROM Cleaned_price_history
WHERE TRUE
  AND (
    --- EXCLUDING TREASURY AND CORPORADE BONDS FOR WHICH I DON'T HAVE DATA AS THEIR VALUE RELY ON TRANSACTIONS
    Instrument_type_id IN (5,7)
    AND adjusted_close IS NOT NULL
    )