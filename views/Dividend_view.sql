-- POBRANIE DANYCH Z WIDOKU TRANSACTIONS --
/*
Pobranie danych z widoku transakcyjnego.
*/

WITH
transactions_view_raw AS (SELECT * FROM `projekt-inwestycyjny.Transactions.Transactions_view`),
daily_data            AS (SELECT * FROM `projekt-inwestycyjny.Dane_instrumentow.Daily`),
calendar              AS (SELECT * FROM `projekt-inwestycyjny.Calendar.Dates`),

-- FILTOWANIE I CASTOWANIE DANYCH --

transactions_view AS (
  SELECT
    *,
    EXTRACT(DATE FROM transaction_timestamp) AS transaction_date
  FROM transactions_view_raw
),


-- PRELIMINARY AGGREGATION --
/*
W kroku tym wyciągany jest rok, miesiąc i dzień wypłaty dywidendy.
Dodatkowo wyznaczana jest łączna suma wartości dywidendy per ticker i jest średnia wartość ze wszystkich wypłat (dla całej dostępnej historii w projekcie)
Oprócz tego dołożone jest połączenie danych dywidend z danymi danych giełdowych - dzięki czemu dokładane są kolumny:
- 'dividend_price' jako wartość dywidendy na jedną sztukę akcji w PLN,
- 'close' jako wartość akcji danego instrumentu na dzień wypłaty dywidendy - dzięki funkcji MAX nie trzeba tych danych agregować - i tak zawsze wychodzi jedna wartość, jako, że dane ściągane są jednokrotnie dla danego tickera w ciągu dnia
*/

preliminary_aggregation AS (
  SELECT
    tv.Project_id                             AS project_id,
    tv.Ticker                                 AS ticker,
    c.year                                    AS year,
    c.month                                   AS month,
    c.day                                     AS day,
    c.quarter_text                            AS quarter,
    tv.transaction_price                      AS dividend_price,
    tv.transaction_value_pln                  AS transaction_value_pln,
    MAX(tv.Unit)                              AS unit,
    MAX(dd.Close)                             AS close,
    SUM(SUM(tv.Transaction_value_pln)) OVER ticker_in_project_window
                                              AS dividend_sum_total_per_ticker,
    AVG(AVG(tv.Transaction_value_pln)) OVER ticker_in_project_window
                                              AS dividend_average_total_per_ticker
  FROM transactions_view  AS tv
  LEFT JOIN daily_data    AS dd
    ON dd.Date = tv.transaction_date
    AND tv.Ticker = dd.Ticker
    AND
      (tv.Project_id = dd.Project_id
      OR dd.Project_id IS NULL)
  LEFT JOIN calendar AS c
    ON tv.transaction_date = c.date
  WHERE TRUE
    AND Transaction_type_group = "Div_related_amount"
  GROUP BY ALL
  WINDOW
    ticker_in_project_window AS (
      PARTITION BY
        tv.Project_id,
        tv.Ticker
    )
),

-- INITIAL AGGREGATION --
/*
W kroku tym wyciągane są wszystkie dane z 'preliminary aggregation'.
Dodatkowo wyznaczane są następujące wskaźniki:
- Skumulowana suma wartości dywidendy per ticker oraz year,
- Skumulowana wartość dywidendy per year,
- Skumulowana wartość dywidendy per year oraz quarter,
- Wartość % dywidendy wobec kursu aktualnego na dzień wypłaty dywidendy
*/

initital_aggregation AS (
  SELECT
    *,
    SUM(transaction_value_pln) OVER(PARTITION BY project_id, ticker, year)    AS dividend_sum_per_ticker_and_year,
    SUM(transaction_value_pln) OVER(PARTITION BY project_id, year)            AS dividend_sum_per_year,
    SUM(transaction_value_pln) OVER(PARTITION BY project_id, year, quarter)   AS dividend_sum_per_year_and_quarter,
    100 * SAFE_DIVIDE(dividend_price, close * unit)                           AS dividend_ratio_pct
  FROM preliminary_aggregation
),

-- MID AGGREGATION VIEW --
/*
W tej cześci wyznaczana jest wartość dywidendy dla danego instrumentu w całym roku wobec wartości dywidendy w poprzednim roku.
Wykorzystywane są wszystkie dane z poprzednich kroków.
Wyznaczona jest również wartość średniego dividend ratio dla danego tickera.
*/


mid_aggregation AS (
  SELECT
    project_id                                    AS project_id,
    ticker                                        AS ticker,
    year                                          AS year,
    month                                         AS month,
    day                                           AS day,
    quarter                                       AS quarter,
    transaction_value_pln                         AS dividend_value_pln,
    dividend_sum_total_per_ticker,
    dividend_sum_per_ticker_and_year,
    dividend_average_total_per_ticker,
    dividend_sum_per_year,
    dividend_sum_per_year_and_quarter,
    IFNULL(
      LAG(
        dividend_sum_per_ticker_and_year
        )
        OVER ticker_in_project_window_ordered, 0)
                                                  AS dividend_value_ticker_last_year,
    IFNULL(dividend_ratio_pct, 0)                 AS dividend_ratio_pct,
    IFNULL
    (
        AVG(dividend_ratio_pct)
        OVER ticker_in_project_window, 0)
                                                  AS avg_dividend_ratio_per_ticker_pct
  FROM initital_aggregation
  WINDOW
    ticker_in_project_window_ordered AS (
      PARTITION BY
        project_id,
        ticker
      ORDER BY
        year
    ),
    ticker_in_project_window AS (
      PARTITION BY
        project_id,
        Ticker
    )
),

-- FINAL AGGREGATION --
/*
W tym kroku dokonywana jest ostatnia analiza - wyznaczenie parametru pokazującego wzrost lub spadek wartości dywidendy dla danego instrumentu, wobec poprzedniego roku.
Dane są posortowane rosnąco po instrumencie finansowym, następnie malejąco po roku oraz kwartale
*/

final_aggregation AS (
  SELECT
    * EXCEPT(day, month),
    IFNULL(
      SAFE_DIVIDE(dividend_sum_per_ticker_and_year, dividend_value_ticker_last_year),
      0) AS dividend_value_change_per_ticker_and_year
  FROM mid_aggregation
)

-- WYCIĄGNIĘCIE DANYCH DO WIDOKU --

SELECT
  project_id,
  ticker,
  year,
  quarter,
  -- Wskaźniki prezentujące dane dla danego projektu i instrumentu --
  dividend_ratio_pct,                   -- Wartość % dywidendy wobec kursu aktualnego dzień wypłaty dywidendy dla danego instrumentu w projekcie
  avg_dividend_ratio_per_ticker_pct,    -- Średnia wartość % dywidendy wobec kursu aktualnego na dzień wypłaty dywidendy dla danego instrumentu w projekcie
  dividend_value_change_per_ticker_and_year, -- Wartość % wzrostu lub spadku dywidendy dla danego instrumentu w projekcie
  dividend_value_pln,                   -- Wartość dywidendy
  dividend_sum_total_per_ticker,        -- Suma wartości wypłaconej dywidendy w całej historii dla danego instrumentu w projekcie
  dividend_sum_per_ticker_and_year,     -- Suma wartości wypłaconej dywidendy w danym roku dla danego instrumentu w projekcie
  dividend_average_total_per_ticker,    -- Średnia wartość wypłaconej dywidendy w całej historii dla danego instrumentu w projekcie
  dividend_value_ticker_last_year,      -- Suma wartości wypłaconej dywidendy w poprzednim roku dla danego instrumentu w projekcie
  -- Wskaźniki prezentujące dane dla danego projektu --
  dividend_sum_per_year,                -- Suma wartości wypłaconej dywidendy w danym roku dla danego projektu
  dividend_sum_per_year_and_quarter     -- Suma wartośći wypłaconej dywidendy w danym roku i kwartale dla danego projektu
FROM final_aggregation