WITH
Tax_calculations AS (SELECT * FROM `projekt-inwestycyjny.Transactions.Tax_calculations`),

/*
Akcje_polskie_transakcje_GPW
Widok przedstawia wszystkie transakcje realizowane w ramach akcji polskich, za pomocą polskiego maklera, wobec instrumentów mających swoją siedzibę w Polsce.
Dane pogrupowane są wg roku wynikającego z daty sprzedaży instrumentu.
*/

Akcje_polskie_transakcje_GPW AS (
  SELECT
    Project_id                                AS ID_Projektu,
    EXTRACT(YEAR FROM Date_sell)              AS Rok_podatkowy,
    'Transakcje polskich instrumentów na GPW' AS Rodzaj_transakcji,
    'Transakcje PIT8C'                        AS Kategoria,
    ROUND(SUM(Tax_deductible_expenses), 2)    AS Koszt_uzyskania_przychodu,
    ROUND(SUM(Income), 2)                     AS Przychod,
    ROUND(SUM(Profit), 2)                     AS Zysk
  FROM Tax_calculations
  WHERE
    TRUE
    AND Transaction_type            = 'Sell'
    AND Currency                    = 'PLN'
    AND Instrument_type             = 'Akcje polskie'
    AND Instrument_headquarter      = 'Polska'
    AND Tax_paid                    IS FALSE
  GROUP BY
    Project_id,
    EXTRACT(YEAR FROM Date_sell),
    Rodzaj_transakcji,
    Kategoria
),


/*
Obligacje_korporacyjne_transakcje
Widok przedstawia wszystkie transakcje realizowane w ramach obligacji korporacyjnych, za pomocą polskiego maklera, wobec instrumentów mających swoją siedzibę w Polsce.
Dane pogrupowane są wg roku wynikającego z daty sprzedaży instrumentu.
*/

Obligacje_korporacyjne_transakcje AS (
  SELECT
    Project_id                                AS ID_Projektu,
    EXTRACT(YEAR FROM Date_sell)              AS Rok_podatkowy,
    'Transakcje polskich instrumentów na GPW' AS Rodzaj_transakcji,
    'Transakcje PIT8C'                        AS Kategoria,
    ROUND(SUM(Tax_deductible_expenses), 2)    AS Koszt_uzyskania_przychodu,
    ROUND(SUM(Income), 2)                     AS Przychod,
    ROUND(SUM(Profit), 2)                     AS Zysk
  FROM Tax_calculations
  WHERE
    TRUE
    AND Transaction_type            = 'Sell'
    AND Currency                    = 'PLN'
    AND Instrument_type             = 'Obligacje korporacyjne'
    AND Instrument_headquarter      = 'Polska'
    AND Tax_paid                    IS FALSE
  GROUP BY
    Project_id,
    EXTRACT(YEAR FROM Date_sell),
    Rodzaj_transakcji,
    Kategoria
),

/*
Akcje_zagraniczne_transakcje_GPW
Widok zawiera wszystkie transakcje realizowane na polskiej giełdzie, w polskiej walucie, których siedziba instrumentów emitenta znajduje się poza polskimi granicami.
*/

Akcje_zagraniczne_transakcje_GPW AS (
  SELECT
    Project_id                                AS ID_Projektu,
    EXTRACT(YEAR FROM Date_sell)              AS Rok_podatkowy,
    'Transakcje zagr. instrumentów na GPW'    AS Rodzaj_transakcji,
    'Transakcje poza PIT 8C'                  AS Kategoria,
    ROUND(SUM(Tax_deductible_expenses), 2)    AS Koszt_uzyskania_przychodu,
    ROUND(SUM(Income), 2)                     AS Przychod,
    ROUND(SUM(Profit), 2)                     AS Zysk,
  FROM Tax_calculations
  WHERE
    TRUE
    AND Transaction_type            = 'Sell'
    AND Currency                    = 'PLN'
    AND Instrument_type             = 'Akcje polskie'
    AND Instrument_headquarter      <> 'Polska'
    AND Tax_paid                    IS FALSE
  GROUP BY
    Project_id,
    EXTRACT(YEAR FROM Date_sell),
    Rodzaj_transakcji,
    Kategoria
),

/*
Akcje_zagraniczne_transakcje_poza_GPW
Widok zawiera wszystkie nieopodatkowane transakcje, w obcej walucie, realizowane na obcej giełdzie.
*/
Akcje_zagraniczne_transakcje_poza_GPW AS (
  SELECT
    Project_id                                AS ID_Projektu,
    EXTRACT(YEAR FROM Date_sell)              AS Rok_podatkowy,
    'Transakcje zagr. instrumentów poza GPW'  AS Rodzaj_transakcji,
    'Transakcje poza PIT 8C'                  AS Kategoria,
    ROUND(SUM(Tax_deductible_expenses), 2)    AS Koszt_uzyskania_przychodu,
    ROUND(SUM(Income), 2)                     AS Przychod,
    ROUND(SUM(Profit), 2)                     AS Zysk
  FROM Tax_calculations
  WHERE
    TRUE
    AND Transaction_type            = 'Sell'
    AND Currency                    <> 'PLN'
    AND Instrument_type             = 'ETF zagraniczne'
    AND Instrument_headquarter      <> 'Polska'
    AND Tax_paid                    IS FALSE
  GROUP BY
    Project_id,
    EXTRACT(YEAR FROM Date_sell),
    Rodzaj_transakcji,
    Kategoria
),

/*
Akcje_polskie_dywidendy_gpw
Widok zawiera wszystkie dywidendy wypłacone na polskiej giełdzie, w obcej walucie, które nie zostały rozliczone.
*/

Akcje_polskie_dywidendy_gpw AS (
  SELECT
    Project_id                                AS ID_Projektu,
    EXTRACT(YEAR FROM Date_sell)              AS Rok_podatkowy,
    'Nierozliczone dywidendy na GPW'          AS Rodzaj_transakcji,
    'Dywidendy zagraniczne'                   AS Kategoria,
    0                                         AS Koszt_uzyskania_przychodu,
    ROUND(SUM(Income), 2)                     AS Przychod,
    ROUND(SUM(Profit), 2)                     AS Zysk
  FROM Tax_calculations
  WHERE
    TRUE
    AND Transaction_type            = 'Dywidenda'
    AND Currency                    <> 'PLN'
    AND Instrument_type             = 'Akcje polskie'
    AND Instrument_headquarter      <> 'Polska'
    AND Tax_paid                    IS FALSE
  GROUP BY
    Project_id,
    EXTRACT(YEAR FROM Date_sell),
    Rodzaj_transakcji,
    Kategoria
),

/*
ETF_zagraniczne_dywidendy_poza_GPW
Widok zawiera wszystkie dywidendy wypłacone na zagranicznej giełdzie, w obcej walucie, które nie zostały rozliczone.
*/

ETF_zagraniczne_dywidendy_poza_GPW AS (
  SELECT
    Project_id                                AS ID_Projektu,
    EXTRACT(YEAR FROM Date_sell)              AS Rok_podatkowy,
    'Dywidendy poza GPW'                      AS Rodzaj_transakcji,
    'Dywidendy zagraniczne'                   AS Kategoria,
    0                                         AS Koszt_uzyskania_przychodu,
    ROUND(SUM(Income), 2)                     AS Przychod,
    ROUND(SUM(Profit), 2)                     AS Zysk
  FROM Tax_calculations
  WHERE
    TRUE
    AND Transaction_type            = 'Dywidenda'
    AND Currency                    <> 'PLN'
    AND Instrument_type             = 'ETF zagraniczne'
    AND Instrument_headquarter      <> 'Polska'
    AND Tax_paid                    IS FALSE
  GROUP BY
    Project_id,
    EXTRACT(YEAR FROM Date_sell),
    Rodzaj_transakcji,
    Kategoria
),

/*
Obligacje_korporacyjne_odsetki
Widok zawiera wszystkie odsetki wypłacone na polskiej giełdzie, ale nierozliczone.
*/

Obligacje_korporacyjne_odsetki AS (
  SELECT
    Project_id                                AS ID_Projektu,
    EXTRACT(YEAR FROM Date_sell)              AS Rok_podatkowy,
    'Odsetki na GPW - obligacje korporacyjne' AS Rodzaj_transakcji,
    'Odsetki polskie'                         AS Kategoria,
    0                                         AS Koszt_uzyskania_przychodu,
    ROUND(SUM(Income), 2)                     AS Przychod,
    ROUND(SUM(Profit), 2)                     AS Zysk
  FROM Tax_calculations
  WHERE
    TRUE
    AND Transaction_type            = 'Odsetki'
    AND Currency                    = 'PLN'
    AND Instrument_type             = 'Obligacje korporacyjne'
    AND Instrument_headquarter      = 'Polska'
    AND Tax_paid                    IS FALSE
  GROUP BY
    Project_id,
    EXTRACT(YEAR FROM Date_sell),
    Rodzaj_transakcji,
    Kategoria
),


/*
data_all_unioned
Widok zawiera wszystkie częściowe rozliczenia.
*/

data_all_unioned AS (
  SELECT *
  FROM Akcje_polskie_transakcje_GPW

  UNION ALL

  SELECT *
  FROM Obligacje_korporacyjne_transakcje

  UNION ALL

  SELECT *
  FROM Akcje_zagraniczne_transakcje_GPW

  UNION ALL
  
  SELECT *
  FROM Akcje_zagraniczne_transakcje_poza_GPW

  UNION ALL

  SELECT *
  FROM Akcje_polskie_dywidendy_gpw 

  UNION ALL

  SELECT *
  FROM ETF_zagraniczne_dywidendy_poza_GPW

  UNION ALL

  SELECT *
  FROM Obligacje_korporacyjne_odsetki
),


/*
data_all_unioned_ordered
Dane analogiczne do powyższych, jednak pofiltrowane wg roku podatkowego oraz rodzaju transakcji.
*/

data_all_unioned_ordered AS (
  SELECT *
  FROM data_all_unioned
  ORDER BY
    ID_Projektu,
    Rok_podatkowy DESC,
    Rodzaj_transakcji
)


--- FINALNY RAPORT ---
SELECT
  ID_Projektu,
  Rok_podatkowy,
  Kategoria,
  ROUND(SUM(Koszt_uzyskania_przychodu), 2)  AS Koszt_uzyskania_przychodow,
  ROUND(SUM(Przychod), 2)                   AS Przychod,
  ROUND(SUM(Zysk), 2)                       AS Zysk,
  ROUND(SUM(Zysk) * 0.19, 2)                AS Podatek_do_zaplaty
FROM data_all_unioned_ordered
GROUP BY
  ID_Projektu,
  Rok_podatkowy,
  Kategoria
ORDER BY
  ID_Projektu,
  Rok_podatkowy DESC,
  Kategoria DESC;


--- TESTY ---

/*
SELECT *
FROM ETF_zagraniczne_dywidendy_poza_GPW
*/

  