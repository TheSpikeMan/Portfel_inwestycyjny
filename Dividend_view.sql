-- POBRANIE DANYCH Z WIDOKU TRANSACTIONS --
/*
Pobranie danych z widoku transakcyjnego.
*/

WITH
Transactions_view AS (SELECT * FROM `projekt-inwestycyjny.Transactions.Transactions_view`),

-- PRELIMINARY AGGREGATION --
/*
W kroku tym wyciągany jest rok, miesiąc i dzień wypłaty dywidendy.
Dodatkowo wyznaczana jest łączna suma wartości dywidendy per ticker i jest średnia wartość ze wszystkich wypłat.
*/

preliminary_aggregation AS (
SELECT
  Ticker AS Ticker,
  EXTRACT(YEAR FROM `Transaction_date`) AS year,
  EXTRACT(MONTH FROM `Transaction_date`) AS month,
  EXTRACT(DAY FROM `Transaction_date`) AS day,
  Quarter AS quarter,
  Transaction_value_pln AS  Transaction_value_pln,
  SUM(SUM(Transaction_value_pln)) OVER(PARTITION BY Ticker) AS divident_sum_total_per_ticker,
  AVG(AVG(Transaction_value_pln)) OVER(PARTITION BY Ticker) AS divident_average_total_per_ticker,
FROM `projekt-inwestycyjny.Transactions.Transactions_view`
WHERE
  Transaction_type = "Dywidenda"
GROUP BY
  1,2,3,4,5,6
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
  SUM(Transaction_value_pln) OVER(PARTITION BY Ticker, year) AS divident_sum_per_ticker_and_year,
  SUM(Transaction_value_pln) OVER(PARTITION BY year) AS divident_sum_per_year,
  SUM(Transaction_value_pln) OVER(PARTITION BY year, quarter) AS divident_sum_per_year_and_quarter
FROM
preliminary_aggregation 
),

-- MID AGGREGATION VIEW --
/*
W tej cześci w głównej mierze dokonywane jest zaokrąglenie wszystkich wskaźników do drugiego miejsca po przecinku.
Dodatkowo wyznaczana jest wartość dywidendy dla danego instrumentu w całym roku wobec wartości dywidendy w poprzednim roku.
Wykorzystywane są wszystkie dane z poprzednich kroków.
*/


mid_aggregation AS(
SELECT
  Ticker,
  year,
  month,
  day,
  quarter,
  ROUND(Transaction_value_pln,2) AS dividend_value_pln,
  ROUND(divident_sum_total_per_ticker,2) AS divident_sum_total_per_ticker,
  ROUND(divident_sum_per_ticker_and_year,2) AS divident_sum_per_ticker_and_year,
  ROUND(divident_average_total_per_ticker,2) AS divident_average_total_per_ticker,
  ROUND(divident_sum_per_year,2) AS divident_sum_per_year,
  ROUND(divident_sum_per_year_and_quarter,2) AS divident_sum_per_year_and_quarter,
  IFNULL(LAG(ROUND(divident_sum_per_ticker_and_year,2)) OVER(PARTITION BY Ticker ORDER BY year ),0) AS dividend_value_ticker_last_year
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
  CONCAT(CAST(IFNULL(ROUND((100* SAFE_DIVIDE(divident_sum_per_ticker_and_year, dividend_value_ticker_last_year) - 100),2),0) AS STRING), "%") AS dividend_value_change_per_ticker_and_year
FROM
  mid_aggregation
ORDER BY
  1, 2 DESC, 3 DESC
)

-- WYCIĄGNIĘCIE DANYCH DO WIDOKU --

SELECT * FROM final_aggregation;



