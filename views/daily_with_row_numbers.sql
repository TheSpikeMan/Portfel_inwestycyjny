CREATE VIEW `projekt-inwestycyjny.Dane_instrumentow.Daily_with_row_numbers`
AS 
(
    SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY Ticker ORDER BY `Date` DESC) AS row_num
    FROM `projekt-inwestycyjny.Dane_instrumentow.Daily`
);
