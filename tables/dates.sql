CREATE OR REPLACE TABLE `projekt-inwestycyjny.Calendar.Dates`
  (
    date           DATE   OPTIONS(description="Pełna data w formacie YYYY-MM-DD"),
    year           INT64  OPTIONS(description="Rok danej daty (np. 2025)"),
    month          INT64  OPTIONS(description="Numer miesiąca (1 = styczeń, 12 = grudzień)"),
    day            INT64  OPTIONS(description="Dzień miesiąca (1–31)"),
    quarter        INT64  OPTIONS(description="Numer kwartału (1–4)"),
    quarter_text   STRING OPTIONS(description="Nazwa kwartału po polsku, np. 'III kwartał'"),
    year_quarter   STRING OPTIONS(description="Połączenie roku i kwartału, np. '2025 Q3'"),
    week           INT64  OPTIONS(description="Numer tygodnia wg ISO 8601 (poniedziałek = 1)"),
    weekday        INT64  OPTIONS(description="Dzień tygodnia jako liczba od 0 (niedziela) do 6 (sobota)"),
    is_working_day INT64  OPTIONS(description="1 = dzień roboczy (poniedziałek–piątek), 0 = weekend")
  ) 
  AS
  SELECT
    dates                              AS date,
    EXTRACT(YEAR FROM dates)           AS year,
    EXTRACT(MONTH FROM dates)          AS month,
    EXTRACT(DAY FROM dates)            AS day,
    EXTRACT(QUARTER FROM dates)        AS quarter,
    CASE
      WHEN EXTRACT(QUARTER FROM dates) = 1 THEN "I kwartał"
      WHEN EXTRACT(QUARTER FROM dates) = 2 THEN "II kwartał"
      WHEN EXTRACT(QUARTER FROM dates) = 3 THEN "III kwartał"
      WHEN EXTRACT(QUARTER FROM dates) = 4 THEN "IV kwartał"
    END                                AS quarter_text,
    CONCAT(
      SAFE_CAST(EXTRACT(YEAR FROM dates) AS STRING),
      " Q",
      SAFE_CAST(EXTRACT(QUARTER FROM dates) AS STRING)
    )                                  AS year_quarter,
    EXTRACT(ISOWEEK FROM dates)        AS week,
    EXTRACT(DAYOFWEEK FROM dates) - 1  AS weekday,
    CASE
      WHEN EXTRACT(DAYOFWEEK FROM dates) - 1
      BETWEEN 1 AND 5
      THEN 1
      ELSE 0
      END                              AS is_working_day
  FROM UNNEST(GENERATE_DATE_ARRAY('2020-01-01', '2030-12-31', INTERVAL 1 DAY) ) AS dates
  ORDER BY
    dates DESC