/* 
Widok agreguje wszystkie dane z kalendarza i łączy je z danymi scrapowanych walut.
Wyznacza również ostatni kurs walutowy na potrzeby obliczeń transakcyjnych. 
*/

WITH
Currency AS (SELECT * FROM `projekt-inwestycyjny.Waluty.Currency`),
Dates AS (SELECT * FROM `projekt-inwestycyjny.Calendar.Dates`),

/*
Pobierz unikatowe transakcje z tabeli z walutami
*/

Unique_currency  AS (
    SELECT DISTINCT Currency.Currency FROM Currency
),

/*
Połącz wszystkie dane kalendarzowe z danymi walut.
*/

Dates_with_currency AS (
    SELECT
        Dates.Date AS Currency_date,
        Currency   AS Currency
    FROM Dates
    CROSS JOIN Unique_currency
),

/*
Utwórz okno oparte o pole waluty i podaj ostatnią nie nullową wartość dla danej kolumny. Jeżeli kolumna jest nie nullowa 
jest podawana. Jeżeli nie, szukana jest najbliższa nienullowa wartość.
*/

Dates_with_non_null_currency AS (
    SELECT
        Dates_with_currency.Currency_date           AS Currency_date,
        Dates_with_currency.Currency                AS Currency,
        LAST_VALUE(Currency_close IGNORE NULLS) OVER last_currency_close AS Currency_close
    FROM Dates_with_currency
    LEFT JOIN Currency
    ON Dates_with_currency.Currency_date = Currency.Currency_date
    AND Dates_with_currency.Currency = Currency.Currency
    WINDOW
        last_currency_close AS (
            PARTITION BY Dates_with_currency.Currency ORDER BY Dates_with_currency.Currency_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
    ORDER BY
        Dates_with_currency.Currency_date
)

/*
Ostatni widok umożliwia wyciągnięcie ostatniej wartości wobec daty w danym wierszu, na potrzeby rozliczenia transakcji.
*/

SELECT
    Currency_date                                   AS Currency_date,
    Currency                                        AS Currency,
    Currency_close                                  AS Currency_close,
    LAG(Currency_close) OVER last_currency_close    AS Last_day_currency
FROM
    Dates_with_non_null_currency
WINDOW
    last_currency_close AS (
        PARTITION BY Currency ORDER BY Currency_date
    )

