-- POBRANIE DANYCH Z WIDOKU TRANSACTIONS --
/*
Pobranie danych z widoku transakcyjnego.
*/

WITH
Transactions_view AS (SELECT * FROM `projekt-inwestycyjny.Transactions.Transactions_view`),
Daily_data        AS (SELECT * FROM `projekt-inwestycyjny.Dane_instrumentow.Daily`),

-- PRELIMINARY AGGREGATION --
/*
W kroku tym wyciągany jest rok, miesiąc i dzień wypłaty dywidendy.
Dodatkowo wyznaczana jest łączna suma wartości dywidendy per ticker i jest średnia wartość ze wszystkich wypłat.
Oprócz tego dołożone jest połączenie danych dywidend z danymi danych giełdowych - dzięki czemu dokładane są kolumny:
- divident_price jako wartość dywidendy na jedną sztukę akcji w PLN,
- MAX(Close) jako wartość akcji danego instrumentu na dzień wypłaty dywidendy - dzięki funkcji MAX nie trzeba tych danych agregować - i tak zawsze wychodzi jedna wartość, jako, że dane ściągane są jednokrotnie dla danego tickera w ciągu dnia
*/

preliminary_aggregation AS (
SELECT
  Project_id                                AS Project_id,
  Transactions_view.Ticker                  AS Ticker,
  EXTRACT(YEAR FROM `Transaction_date`)     AS year,
  EXTRACT(MONTH FROM `Transaction_date`)    AS month,
  EXTRACT(DAY FROM `Transaction_date`)      AS day,
  Quarter                                   AS quarter,
  ROUND((Transaction_value_pln/Transaction_amount), 2) 
                                            AS divident_price,
  Transaction_value_pln AS  Transaction_value_pln,
  MAX(Unit)                                 AS unit,
  MAX(Daily_data.Close)                     AS close,
  SUM(SUM(Transaction_value_pln)) OVER(PARTITION BY Transactions_view.Ticker) 
                                            AS divident_sum_total_per_ticker,
  AVG(AVG(Transaction_value_pln)) OVER(PARTITION BY Transactions_view.Ticker) 
                                            AS divident_average_total_per_ticker
FROM Transactions_view
LEFT JOIN Daily_data
ON Daily_data.Date = Transactions_view.Transaction_date 
AND Transactions_view.Ticker = Daily_data.Ticker
WHERE TRUE
  AND Transaction_type_group = "Div_related_amount"
GROUP BY ALL
),

-- INITIAL AGGREGATION --
/*
W kroku tym wyciągane są wszystkie dane z 'preliminary aggregation'.
Dodatkowo wyznaczane są następujące wskaźniki:
- Skumulowana suma wartości dywidendy per ticker oraz year,
- Skumulowana wartość dywidendy per year,
- Skumulowana wartość dywidendy per year oraz quarter.
*/

initital_aggregation AS (
SELECT
  *,
  SUM(Transaction_value_pln) OVER(PARTITION BY Ticker, year)    AS divident_sum_per_ticker_and_year,
  SUM(Transaction_value_pln) OVER(PARTITION BY year)            AS divident_sum_per_year,
  SUM(Transaction_value_pln) OVER(PARTITION BY year, quarter)   AS divident_sum_per_year_and_quarter,
  ROUND(100 * SAFE_DIVIDE(divident_price, close * unit), 2)     AS dividend_ratio_pct
FROM preliminary_aggregation 
),

-- MID AGGREGATION VIEW --
/*
W tej cześci w głównej mierze dokonywane jest zaokrąglenie wszystkich wskaźników do drugiego miejsca po przecinku.
Dodatkowo wyznaczana jest wartość dywidendy dla danego instrumentu w całym roku wobec wartości dywidendy w poprzednim roku.
Wykorzystywane są wszystkie dane z poprzednich kroków.
Wyznaczona jest również wartość średniego divident ratio dla danego tickera.
*/


mid_aggregation AS (
SELECT
  Project_id                                    AS Project_id,
  Ticker                                        AS Ticker,
  year                                          AS year,
  month                                         AS month,
  day                                           AS day,
  quarter                                       AS quarter,
  ROUND(Transaction_value_pln,2)                AS dividend_value_pln,
  ROUND(divident_sum_total_per_ticker,2)        AS divident_sum_total_per_ticker,
  ROUND(divident_sum_per_ticker_and_year,2)     AS divident_sum_per_ticker_and_year,
  ROUND(divident_average_total_per_ticker,2)    AS divident_average_total_per_ticker,
  ROUND(divident_sum_per_year,2)                AS divident_sum_per_year,
  ROUND(divident_sum_per_year_and_quarter,2)    AS divident_sum_per_year_and_quarter,
  IFNULL(LAG(ROUND(divident_sum_per_ticker_and_year,2)) OVER(PARTITION BY Ticker ORDER BY year ),0) 
                                                AS dividend_value_ticker_last_year,
  IFNULL(dividend_ratio_pct, 0)                 AS dividend_ratio_pct,
  IFNULL(ROUND(AVG(dividend_ratio_pct) OVER(PARTITION BY Ticker),2) , 0) 
                                                AS avg_dividend_ratio_per_ticker_pct
FROM initital_aggregation
),

-- FINAL AGGREGATION --
/*
W tym kroku dokonywana jest ostatnia analiza - wyznaczenie parametru pokazującego wzrost lub spadek wartości dywidendy dla danego instrumentu, wobec poprzedniego roku.
Dane są posortowane rosnąco po instrumencie finansowym, następnie malejąco po roku oraz kwartale
*/

final_aggregation AS (
SELECT
  * EXCEPT(day, month),
  CONCAT(
    CAST(
      IFNULL(
        ROUND((100* SAFE_DIVIDE(divident_sum_per_ticker_and_year, dividend_value_ticker_last_year) - 100),2),0) 
    AS STRING), "%") AS dividend_value_change_per_ticker_and_year
FROM
  mid_aggregation
ORDER BY
  1, 2 DESC, 3 DESC
)

-- WYCIĄGNIĘCIE DANYCH DO WIDOKU --

SELECT * FROM final_aggregation;



