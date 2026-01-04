WITH
raw_scraped_data_raw AS (SELECT * FROM `projekt-inwestycyjny.Raw_data.Raw_scraped_data`),
raw_scraped_data     AS (
  SELECT
    Ticker,
    Data,
    Miara,
    Okres,
    SAFE_CAST(REGEXP_EXTRACT(Raport, r'^(\d{4})') AS INT64) AS Rok,
    SAFE_CAST(REGEXP_EXTRACT(Raport, r'Q(\d+)') AS INT64)   AS Kwartal,
    REGEXP_REPLACE(                             -- 3. usuń _ na końcu
      REGEXP_REPLACE(                           -- 2. zredukuj wielokrotne _
        REGEXP_REPLACE(                         -- 1. zamień znaki na _
          REGEXP_REPLACE(
            NORMALIZE(
              CASE
                WHEN Dane = 'r/r' THEN CONCAT(Miara, '_r_r')
                WHEN Dane = 'k/k' THEN CONCAT(Miara, '_k_k')
                ELSE Dane
              END,
              NFD
            ),
            r'\pM',
            ''
          ),
          r'[ /\\()\-~,.]',
          '_'
        ),
        r'_+',                                  -- <<< wielokrotne _
        '_'
      ),
      r'^_|_$',                                 -- <<< _ na początku lub końcu
      ''
    ) AS Dane,
    Wartosc
  FROM raw_scraped_data_raw
  WHERE TRUE
    AND Rynek = 'gpw'
    AND Typ_raportu IN ("Raporty_finansowe", "Wskaźniki")
    AND Typ_subraportu NOT IN ("Bilans")
  QUALIFY TRUE
    AND ROW_NUMBER() OVER latest_data = 1
  WINDOW
    latest_data AS (
      PARTITION BY
        Ticker,
        Profil,
        Typ_raportu,
        Typ_subraportu,
        Miara,
        Okres,
        Raport,
        Rynek,
        Dane
      ORDER BY
        Data DESC,
        Timestamp DESC
    )
),

base_prepared AS (
  SELECT
    Ticker                                                          AS Ticker,
    Data                                                            AS Data_aktualizacji,
    CASE
      WHEN Kwartal = 1 THEN DATE(Rok, 3, 31)
      WHEN Kwartal = 2 THEN DATE(Rok, 6, 30)
      WHEN Kwartal = 3 THEN DATE(Rok, 9, 30)
      WHEN Kwartal = 4 THEN DATE(Rok, 12, 31)
    END                                                             AS Data_konca_kwartalu,
    Rok                                                             AS Rok,
    Kwartal                                                         AS Kwartal,
    Okres                                                           AS Okres,
    Dane                                                            AS Dane,
    Wartosc                                                         AS Wartosc
  FROM raw_scraped_data AS rsc
)

SELECT
  *
FROM base_prepared
PIVOT(
  ANY_VALUE(Wartosc)
  FOR Dane IN (
    'Dywidenda',
    'Dywidenda_k_k',
    'Dywidenda_r_r',
    'EBITDA',
    'EBITDA_k_k',
    'EBITDA_r_r',
    'Koszt_ogolnego_zarzadu_k_k',
    'Koszt_ogolnego_zarzadu_r_r',
    'Koszt_sprzedazy_k_k',
    'Koszt_sprzedazy_r_r',
    'Koszt_wytworzenia_produkcji_sprzedanej_k_k',
    'Koszt_wytworzenia_produkcji_sprzedanej_r_r',
    'Koszty_finansowe',
    'Koszty_finansowe_k_k',
    'Koszty_finansowe_r_r',
    'Koszty_ogolnego_zarzadu',
    'Koszty_sprzedazy',
    'Pozostale_koszty_operacyjne_k_k',
    'Pozostale_koszty_operacyjne_r_r',
    'Pozostale_przychody_koszty_k_k',
    'Pozostale_przychody_koszty_r_r',
    'Pozostale_przychody_operacyjne_k_k',
    'Pozostale_przychody_operacyjne_r_r',
    'Pozostałe_koszty_operacyjne',
    'Pozostałe_przychody_koszty',
    'Pozostałe_przychody_operacyjne',
    'Przeplywy_pieniezne_razem_k_k',
    'Przeplywy_pieniezne_razem_r_r',
    'Przeplywy_pieniezne_z_dzialalnosci_finansowej_k_k',
    'Przeplywy_pieniezne_z_dzialalnosci_finansowej_r_r',
    'Przeplywy_pieniezne_z_dzialalnosci_inwestycyjnej_k_k',
    'Przeplywy_pieniezne_z_dzialalnosci_inwestycyjnej_r_r',
    'Przeplywy_pieniezne_z_dzialalnosci_operacyjnej_k_k',
    'Przeplywy_pieniezne_z_dzialalnosci_operacyjnej_r_r',
    'Przepływy_pieniezne_razem',
    'Przepływy_pieniezne_z_działalnosci_finansowej',
    'Przepływy_pieniezne_z_działalnosci_inwestycyjnej',
    'Przepływy_pieniezne_z_działalnosci_operacyjnej',
    'Przychody_finansowe',
    'Przychody_finansowe_k_k',
    'Przychody_finansowe_r_r',
    'Przychody_ze_sprzedazy',
    'Przychody_ze_sprzedazy_k_k',
    'Przychody_ze_sprzedazy_r_r',
    'Techniczny_koszt_wytworzenia_produkcji_sprzedanej',
    'Zysk_netto',
    'Zysk_netto_akcjonariuszy_jednostki_dominujacej',
    'Zysk_netto_akcjonariuszy_jednostki_dominujacej_k_k',
    'Zysk_netto_akcjonariuszy_jednostki_dominujacej_r_r',
    'Zysk_netto_k_k',
    'Zysk_netto_r_r',
    'Zysk_operacyjny_EBIT',
    'Zysk_operacyjny_k_k',
    'Zysk_operacyjny_r_r',
    'Zysk_przed_opodatkowaniem',
    'Zysk_przed_opodatkowaniem_k_k',
    'Zysk_przed_opodatkowaniem_r_r',
    'Zysk_strata_netto_z_działalnosci_zaniechanej',
    'Zysk_strata_z_dzialalnosci_gospodarczej_k_k',
    'Zysk_strata_z_dzialalnosci_gospodarczej_r_r',
    'Zysk_z_dzialalnosci_gospodarczej_k_k',
    'Zysk_z_dzialalnosci_gospodarczej_r_r',
    'Zysk_z_działalnosci_gospodarczej',
    'Zysk_ze_sprzedazy',
    'Zysk_ze_sprzedazy_k_k',
    'Zysk_ze_sprzedazy_r_r'
  )
)
WHERE TRUE
ORDER BY Ticker, Data_konca_kwartalu DESC