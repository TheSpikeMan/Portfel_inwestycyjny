-- DATA IMPORT --
/*
W pierwszym kroku pobranie danych podatkowych oraz danych instrumentów.
*/

WITH 
Tax_calculations AS (SELECT * FROM `projekt-inwestycyjny.Transactions.Tax_calculations`),
Instruments_types AS (SELECT * FROM `projekt-inwestycyjny.Dane_instrumentow.Instrument_types`),
Instruments AS (SELECT * FROM `projekt-inwestycyjny.Dane_instrumentow.Instruments`),

instruments_with_types AS (
    SELECT
        Instruments.Instrument_id AS Ticker_id,
        Instruments_types.Instrument_type_id AS Instrument_type_id
    FROM Instruments
    LEFT JOIN Instruments_types
    ON Instruments.Instrument_type_id = Instruments_types.Instrument_type_id
),

-- DATA TRANSFORMATION --
/*
W drugim kroku dostosowanie formatu danych podatkowych.
*/

data_with_instrument_types AS (
    SELECT
        Date_sell AS Date_sell,
        Instrument_type_id AS Instrument_type_id,
        Currency AS Currency,
        Tax_Deductible_Expenses AS Tax_Deductible_Expenses,
        Income AS Income,
        Profit AS Profit
    FROM Tax_calculations
    LEFT JOIN instruments_with_types
    ON Tax_calculations.Ticker_id = instruments_with_types.Ticker_id

    /*
    Do zweryfikowania w jakich przypadkach mamy do czynienia z zapłaconym podatkiem
    w obligacjach korporacyjnych, a w jakim nie.
    */
    WHERE Instrument_type_id <> 7
),


-- DATA ADJUSTED --
/*
W kroku tym dane agregowane są do poziomu roku i sumowane.
*/
data_adjusted AS (
    SELECT
        EXTRACT(YEAR FROM Date_sell) AS transaction_year,
        /*
        CASE 
            WHEN Currency = 'PLN' THEN 'GPW'
            ELSE 'Rynki zagraniczne'
        END AS market,
        */
        ROUND(SUM(tax_deductible_expenses), 2) AS tax_deductible_expenses,
        ROUND(SUM(Income), 2) AS income,
        ROUND(SUM(Profit), 2) AS profit
    FROM 
        data_with_instrument_types
    GROUP BY
        transaction_year
        --market
    ORDER BY
        transaction_year
)

SELECT *
FROM data_adjusted