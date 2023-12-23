-- Pobranie wszystkich danych -- 
-- Pobranie wszystkich danych z tabeli przechowującej dane giełdowe instrumentów finansowych --

WITH 
daily AS (SELECT * FROM `projekt-inwestycyjny.Dane_instrumentow.Daily` ORDER BY `Date` ASC),


-- Dodanie średniej 5-cio dniowej, średniej 15-dniowej, średniej 70-dniowej --
-- Średnie zostały dodane za pomocą trzech okien, gdzie głównym kryterium podziału jest Ticker --

added_averages AS (
  SELECT
    *,
    ROUND(AVG(Close) OVER(PARTITION BY Ticker ORDER BY `Date` ROWS 4 PRECEDING), 2)       AS moving_average_5,
    ROUND(AVG(Close) OVER(PARTITION BY Ticker ORDER BY `Date` ROWS 14 PRECEDING), 2)      AS moving_average_15,
    ROUND(AVG(Close) OVER(PARTITION BY Ticker ORDER BY `Date` ROWS 69 PRECEDING), 2)      AS moving_average_70,
  FROM
    daily
),


-- Wyznaczenie największej z wartości średnich, celem wyznaczenia trendu --

added_greatest AS (
  SELECT
    *,
    GREATEST(moving_average_5, moving_average_15, moving_average_70) AS greatest_avg
  FROM
    added_averages
),

-- Dodanie informacji o trendzie --
-- Dzięki funkcji ROW_NUMBER () przydzielono numerację wszystkim instrumentom i wyciągnięcie tylko ostatnich danych (w kolejnym kroku odbywa się filtrowanie po pierwszym (najnowszym) wierszu dla danego instrumentu)

final_aggregation AS (
  SELECT
  *,
  CASE
    WHEN greatest_avg = moving_average_5 THEN "Trend spadkowy"
    WHEN greatest_avg = moving_average_15 THEN "Krótkotrwały trend wzrostowy"
    WHEN greatest_avg = moving_average_70 THEN "Długotrwały trend wzrostowy"
    ELSE "Unknown"
  END AS trend,
  ROW_NUMBER() OVER (PARTITION BY Ticker ORDER BY `Date` DESC) as row_num
  FROM
    added_greatest
)

-- Wyciągięcie danych do widoku--


SELECT
  Ticker,
  trend
FROM
  final_aggregation
WHERE
  row_num = 1;


