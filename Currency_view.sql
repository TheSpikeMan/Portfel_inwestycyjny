/* 
Widok agreguje wszystkie dane z kalendarza i łączy je z danymi scrapowanych walut.
Wyznacza również ostatni kurs walutowy na potrzeby obliczeń transakcyjnych. 
*/

WITH
currency    AS (SELECT * FROM `projekt-inwestycyjny.Waluty.Currency`),
dates       AS (SELECT * FROM `projekt-inwestycyjny.Calendar.Dates`),

/*
Pobierz unikatowe transakcje z tabeli z walutami
*/

unique_currency  AS (
    SELECT DISTINCT 
        currency.Currency 
    FROM currency
),

/*
Połącz wszystkie dane kalendarzowe z danymi walut.
*/

dates_with_currency AS (
    SELECT
        dates.Date AS Currency_date,
        Currency   AS Currency
    FROM dates
    CROSS JOIN unique_currency
),

/*
Utwórz okno oparte o pole waluty i podaj ostatnią nie nullową wartość dla danej kolumny. Jeżeli kolumna jest nie nullowa 
jest podawana. Jeżeli nie, szukana jest najbliższa nienullowa wartość.
*/

dates_with_non_null_currency AS (
    SELECT
        Dates_with_currency.Currency_date           AS Currency_date,
        Dates_with_currency.Currency                AS Currency,
        LAST_VALUE(Currency_close IGNORE NULLS) OVER last_currency_close AS Currency_close
    FROM dates_with_currency
    LEFT JOIN currency
    ON dates_with_currency.Currency_date = currency.Currency_date
    AND dates_with_currency.Currency = currency.Currency
    WINDOW
        last_currency_close AS (
            PARTITION BY Dates_with_currency.Currency ORDER BY Dates_with_currency.Currency_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
    ORDER BY
        dates_with_currency.Currency_date
)

/*
Ostatni widok umożliwia wyciągnięcie ostatniej wartości wobec daty w danym wierszu, na potrzeby rozliczenia transakcji.
*/

SELECT
    SAFE_CAST(Currency_date AS STRING)              AS Currency_date,    --> Potencjalna data transakcji
    Currency                                        AS Currency,         --> Waluta
    Currency_close                                  AS Currency_close,   --> Kurs zamknięcia na dany dzień
    LAG(Currency_close) OVER last_currency_close    AS Last_day_currency --> Kurs zamknięcia na ostatni dzień roboczy
FROM
    dates_with_non_null_currency
WINDOW
    last_currency_close AS (
        PARTITION BY Currency ORDER BY Currency_date
    )

