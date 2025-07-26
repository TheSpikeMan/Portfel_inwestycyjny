WITH
tax_calculations_raw AS (SELECT * FROM `projekt-inwestycyjny.Transactions.Tax_calculations`),


/*
Filtrowanie danych i wstępne kalkulacje
*/

tax_calculations AS (
  SELECT
    Project_id                                AS ID_projektu,
    Transaction_type                          AS Typ_transakcji,
    Instrument_headquarter                    AS Siedziba,
    Currency                                  AS Waluta,
    Instrument_type                           AS Typ_instrumentu,
    Tax_paid                                  AS Zaplacono_podatek,
    EXTRACT(year FROM Date_sell)              AS Rok_podatkowy,
    Tax_deductible_expenses                   AS Koszt_uzyskania_przychodu,
    Income                                    AS Przychod,
    Profit                                    AS Zysk
  FROM tax_calculations_raw
),

/*
Akcje_polskie_transakcje_GPW
Widok przedstawia wszystkie transakcje realizowane w ramach akcji polskich, za pomocą polskiego maklera, wobec instrumentów mających swoją siedzibę w Polsce.
Dane pogrupowane są wg roku wynikającego z daty sprzedaży instrumentu.
*/

Akcje_polskie_transakcje_GPW AS (
  SELECT
    ID_projektu                               AS ID_Projektu,
    Rok_podatkowy                             AS Rok_podatkowy,
    'Transakcje polskich instrumentów na GPW' AS Rodzaj_transakcji,
    'Transakcje PIT8C'                        AS Kategoria,
    SUM(Koszt_uzyskania_przychodu)            AS Koszt_uzyskania_przychodu,
    SUM(Przychod)                             AS Przychod,
    SUM(Zysk)                                 AS Zysk
  FROM Tax_calculations
  WHERE
    TRUE
    AND Typ_transakcji              = 'Sell'
    AND Waluta                      = 'PLN'
    AND Typ_instrumentu             = 'Akcje polskie'
    AND Siedziba                    = 'Polska'
    AND Zaplacono_podatek           IS FALSE
  GROUP BY
    ID_projektu,
    Rok_podatkowy,
    Rodzaj_transakcji,
    Kategoria
),


/*
ETF_polskie_GPW
Widok przedstawia wszystkie transakcje realizowane w ramach ETF polskich, za pomocą polskiego maklera, wobec instrumentów mających swoją siedzibę w Polsce.
Dane pogrupowane są wg roku wynikającego z daty sprzedaży instrumentu.
*/

ETF_polskie_GPW AS (
  SELECT
    ID_projektu                               AS ID_Projektu,
    Rok_podatkowy                             AS Rok_podatkowy,
    'Transakcje polskich instrumentów na GPW' AS Rodzaj_transakcji,
    'Transakcje PIT8C'                        AS Kategoria,
    SUM(Koszt_uzyskania_przychodu)            AS Koszt_uzyskania_przychodu,
    SUM(Przychod)                             AS Przychod,
    SUM(Zysk)                                 AS Zysk
  FROM Tax_calculations
  WHERE
    TRUE
    AND Typ_transakcji              = 'Sell'
    AND Waluta                      = 'PLN'
    AND Typ_instrumentu             = 'ETF obligacyjne polskie'
    AND Siedziba                    = 'Polska'
    AND Zaplacono_podatek           IS FALSE
  GROUP BY ALL
),


/*
Obligacje_korporacyjne_transakcje
Widok przedstawia wszystkie transakcje realizowane w ramach obligacji korporacyjnych, za pomocą polskiego maklera, wobec instrumentów mających swoją siedzibę w Polsce.
Dane pogrupowane są wg roku wynikającego z daty sprzedaży instrumentu.
*/

Obligacje_korporacyjne_transakcje AS (
  SELECT
    ID_projektu                               AS ID_Projektu,
    Rok_podatkowy                             AS Rok_podatkowy,
    'Transakcje polskich instrumentów na GPW' AS Rodzaj_transakcji,
    'Transakcje PIT8C'                        AS Kategoria,
    SUM(Koszt_uzyskania_przychodu)            AS Koszt_uzyskania_przychodu,
    SUM(Przychod)                             AS Przychod,
    SUM(Zysk)                                 AS Zysk
  FROM Tax_calculations
  WHERE
    TRUE
    AND Typ_transakcji              = 'Sell'
    AND Waluta                      = 'PLN'
    AND Typ_instrumentu             = 'Obligacje korporacyjne'
    AND Siedziba                    = 'Polska'
    AND Zaplacono_podatek           IS FALSE
  GROUP BY
    ID_projektu,
    Rok_podatkowy,
    Rodzaj_transakcji,
    Kategoria
),

/*
Akcje_zagraniczne_transakcje_GPW
Widok zawiera wszystkie transakcje realizowane na polskiej giełdzie, w polskiej walucie, których siedziba instrumentów emitenta znajduje się poza polskimi granicami.
*/

Akcje_zagraniczne_transakcje_GPW AS (
  SELECT
    ID_projektu                               AS ID_Projektu,
    Rok_podatkowy                             AS Rok_podatkowy,
    'Transakcje zagr. instrumentów na GPW'    AS Rodzaj_transakcji,
    'Transakcje poza PIT 8C'                  AS Kategoria,
    SUM(Koszt_uzyskania_przychodu)            AS Koszt_uzyskania_przychodu,
    SUM(Przychod)                             AS Przychod,
    SUM(Zysk)                                 AS Zysk
  FROM Tax_calculations
  WHERE
    TRUE
    AND Typ_transakcji              = 'Sell'
    AND Waluta                      = 'PLN'
    AND Typ_instrumentu             = 'Akcje polskie'
    AND Siedziba                    <> 'Polska'
    AND Zaplacono_podatek           IS FALSE
  GROUP BY
    ID_projektu,
    Rok_podatkowy,
    Rodzaj_transakcji,
    Kategoria
),

/*
ETF_zagraniczne_transakcje_poza_GPW
Widok zawiera wszystkie nieopodatkowane transakcje, w obcej walucie, realizowane na obcej giełdzie. Transakcje realizowane są u polskiego maklera, więc rozliczone w ramach PIT8C.
*/

ETF_zagraniczne_transakcje_poza_GPW AS (
  SELECT
    ID_projektu                               AS ID_Projektu,
    Rok_podatkowy                             AS Rok_podatkowy,
    'Transakcje zagr. instrumentów poza GPW'  AS Rodzaj_transakcji,
    'Transakcje PIT8C'                        AS Kategoria,
    SUM(Koszt_uzyskania_przychodu)            AS Koszt_uzyskania_przychodu,
    SUM(Przychod)                             AS Przychod,
    SUM(Zysk)                                 AS Zysk
  FROM Tax_calculations
  WHERE
    TRUE
    AND Typ_transakcji              = 'Sell'
    AND Waluta                      <> 'PLN'
    AND Typ_instrumentu             = 'ETF akcyjne zagraniczne'
    AND Siedziba                    <> 'Polska'
    AND Zaplacono_podatek           IS FALSE
  GROUP BY
    ID_projektu,
    Rok_podatkowy,
    Rodzaj_transakcji,
    Kategoria
),

/*
Akcje_polskie_dywidendy_gpw
Widok zawiera wszystkie dywidendy wypłacone na polskiej giełdzie, w obcej walucie, które nie zostały rozliczone.
*/

Akcje_polskie_dywidendy_gpw AS (
  SELECT
    ID_projektu                               AS ID_Projektu,
    Rok_podatkowy                             AS Rok_podatkowy,
    'Nierozliczone dywidendy na GPW'          AS Rodzaj_transakcji,
    'Dywidendy zagraniczne'                   AS Kategoria,
    0                                         AS Koszt_uzyskania_przychodu,
    SUM(Przychod)                             AS Przychod,
    SUM(Zysk)                                 AS Zysk
  FROM Tax_calculations
  WHERE
    TRUE
    AND Typ_transakcji               = 'Dywidenda'
    AND Waluta                      <> 'PLN'
    AND Typ_instrumentu             = 'Akcje polskie'
    AND Siedziba                    <> 'Polska'
    AND Zaplacono_podatek           IS FALSE
  GROUP BY
    ID_projektu,
    Rok_podatkowy,
    Rodzaj_transakcji,
    Kategoria
),

/*
ETF_zagraniczne_dywidendy_poza_GPW
Widok zawiera wszystkie dywidendy wypłacone na zagranicznej giełdzie, w obcej walucie, które nie zostały rozliczone.
*/

ETF_zagraniczne_dywidendy_poza_GPW AS (
  SELECT
    ID_projektu                               AS ID_Projektu,
    Rok_podatkowy                             AS Rok_podatkowy,
    'Dywidendy poza GPW'                      AS Rodzaj_transakcji,
    'Dywidendy zagraniczne'                   AS Kategoria,
    0                                         AS Koszt_uzyskania_przychodu,
    SUM(Przychod)                             AS Przychod,
    SUM(Zysk)                                 AS Zysk
  FROM Tax_calculations
  WHERE
    TRUE
    AND Typ_transakcji               = 'Dywidenda'
    AND Waluta                      <> 'PLN'
    AND Typ_instrumentu             = 'ETF akcyjne zagraniczne'
    AND Siedziba                    <> 'Polska'
    AND Zaplacono_podatek           IS FALSE
  GROUP BY
    ID_projektu,
    Rok_podatkowy,
    Rodzaj_transakcji,
    Kategoria
),

/*
Obligacje_korporacyjne_odsetki
Widok zawiera wszystkie odsetki wypłacone na polskiej giełdzie, ale nierozliczone.
*/

Obligacje_korporacyjne_odsetki AS (
  SELECT
    ID_projektu                               AS ID_Projektu,
    Rok_podatkowy                             AS Rok_podatkowy,
    'Odsetki na GPW - obligacje korporacyjne' AS Rodzaj_transakcji,
    'Odsetki polskie'                         AS Kategoria,
    0                                         AS Koszt_uzyskania_przychodu,
    SUM(Przychod)                             AS Przychod,
    SUM(Zysk)                                 AS Zysk
  FROM Tax_calculations
  WHERE
    TRUE
    AND Typ_transakcji              = 'Odsetki'
    AND Waluta                      = 'PLN'
    AND Typ_instrumentu             = 'Obligacje korporacyjne'
    AND Siedziba                    = 'Polska'
    AND Zaplacono_podatek           IS FALSE
  GROUP BY
    ID_projektu,
    Rok_podatkowy,
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
  FROM ETF_polskie_GPW

  UNION ALL

  SELECT *
  FROM Obligacje_korporacyjne_transakcje

  UNION ALL

  SELECT *
  FROM Akcje_zagraniczne_transakcje_GPW

  UNION ALL
  
  SELECT *
  FROM ETF_zagraniczne_transakcje_poza_GPW

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
)


--- FINALNY RAPORT ---
SELECT
  ID_Projektu,
  Rok_podatkowy                                             AS Rok_podatkowy,
  Kategoria                                                 AS Kategoria,
  ROUND(SUM(Przychod), 2)                                   AS Przychod,
  ROUND(SUM(Koszt_uzyskania_przychodu), 2)                  AS Koszt_uzyskania_przychodow,
  ROUND(SUM(Przychod) - SUM(Koszt_uzyskania_przychodu), 2)  AS Dochod,
  ROUND(SUM(Zysk), 2)                                       AS Zysk,
  CASE
    WHEN Kategoria IN ("Dywidendy zagraniczne", "Odsetki polskie")
    THEN ROUND(SUM(Zysk) * 0.19, 0)                         --> Podatek od dywidend zaokrąglam do pełnych wartości
    ELSE ROUND(SUM(Zysk) * 0.19, 2)  
  END                                                       AS Podatek_do_zaplaty
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