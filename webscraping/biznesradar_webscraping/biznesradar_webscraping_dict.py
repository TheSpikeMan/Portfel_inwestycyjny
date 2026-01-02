
""" Słownik rynków """
market_dict = {'gpw': 'akcje_gpw', 'etf': 'etf'}

""" Słownik raportów """
reports_dict = {
    'Raporty_finansowe':
                 [{'Rachunek_zyskow_i_strat': 'spolki-raporty-finansowe-rachunek-zyskow-i-strat'},
                  {'Bilans': 'spolki-raporty-finansowe-bilans'},
                  {'Przeplywy_pieniezne': 'spolki-raporty-finansowe-przeplywy-pieniezne'}],
    'Wskazniki':
                  [{'Wartosci_rynkowej': 'spolki-wskazniki-wartosci-rynkowej'},
                   {'Rentownosci': 'spolki-wskazniki-rentownosci'},
                   {'Przeplywow_pienieznych': 'spolki-wskazniki-przeplywow-pienieznych'},
                   {'Zadluzenia': 'spolki-wskazniki-zadluzenia'},
                   {'Plynnosci': 'spolki-wskazniki-plynnosci'},
                   {'Aktywnosci': 'spolki-wskazniki-aktywnosci'}],
    'Rating': [{'Rating': 'spolki-rating'}]
    }

""" Słownik subraportów """
data_dict = {
    'Rachunek_zyskow_i_strat':
                 [{'Przychody_ze_sprzedazy': 'IncomeRevenues'},
                  {'Koszt_wytworzenia_produkcji_sprzedanej': 'IncomeCostOfSales'},
                  {'Koszt_sprzedazy': 'IncomeDistributionExpenses'},
                  {'Koszt_ogolnego_zarzadu': 'IncomeAdministrativExpenses'},
                  {'Zysk_ze_sprzedazy': 'IncomeGrossProfit'},
                  {'Pozostale_przychody_operacyjne': 'IncomeOtherOperatingIncome'},
                  {'Pozostale_koszty_operacyjne': 'IncomeOtherOperatingCosts'},
                  {'Zysk_operacyjny': 'IncomeEBIT'},
                  {'EBITDA': 'IncomeEBITDA'},
                  {'Przychody_finansowe': 'IncomeFinanceIncome'},
                  {'Koszty_finansowe': 'IncomeFinanceCosts'},
                  {'Pozostale_przychody_koszty': 'IncomeOtherIncome'},
                  {'Zysk_z_dzialalnosci_gospodarczej': 'IncomeNetGrossProfit'},
                  {'Zysk_przed_opodatkowaniem': 'IncomeBeforeTaxProfit'},
                  {'Zysk_strata_z_dzialalnosci_gospodarczej': 'IncomeDiscontinuedProfit'},
                  {'Zysk_netto': 'IncomeNetProfit'},
                  {'Zysk_netto_akcjonariuszy_jednostki_dominujacej': 'IncomeShareholderNetProfit'}],
    'Bilans':
                 [{'Aktywa_razem': 'BalanceTotalAssets'},
                  {'Aktywa_trwale_razem': 'BalanceNoncurrentAssets'},
                  {'Aktywa_trwale_wartosci_niematerialne_i_prawne': 'BalanceIntangibleAssets'},
                  {'Aktywa_trwale_rzeczowe_skladniki_majatku_trwalego': 'BalanceProperty'},
                  {'Aktywa_trwale_aktywa_z_tytulu_prawa_do_uzytkowania': 'BalanceRightToUseAssets'},
                  {'Aktywa_trwale_naleznosci_dlugoterminowe': 'BalanceNoncurrentReceivables'},
                  {'Aktywa_trwale_inwestycyjne_dlugoterminowe': 'BalanceNoncurrInvestments'},
                  {'Aktywa_trwale_pozostale_aktywa_trwale': 'BalanceOtherNoncurrentAssets'},
                  {'Aktywa_obrotowe_razem': 'BalanceCurrentAssets'},
                  {'Aktywa_obrotowe_zapasy': 'BalanceInventory'},
                  {'Aktywa_obrotowe_naleznosci_krotkoterminowe': 'BalanceCurrentReceivables'},
                  {'Aktywa_obrotowe_inwestycje_krotkoterminowe': 'BalanceCurrentInvestments'},
                  {'Aktywa_obrotowe_pozostale_aktywa_obrotowe': 'BalanceOtherCurrentAssets'},
                  {'Aktywa_obrotowe_aktywa_trwale_przeznaczone_do_sprzedazy': 'BalanceAssetsForSale'},
                  {'Kapital_wlasny_akcjonariuszy_jednostki_dominujacej': 'BalanceCapital'},
                  {'Zysk_strata_z_lat_ubieglych': 'BalanceRetainedEarnings'},
                  {'Zysk_strata_netto': 'BalanceYearProfit'},
                  {'Zobowiazania_dlugoterminowe': 'BalanceNoncurrentLiabilities'},
                  {'Zobowiazania_krotkoterminowe': 'BalanceCurrentLiabilities'},
                  {'Pasywa_razem': 'BalanceTotalEquityAndLiabilities'}],
    'Przeplywy_pieniezne':
                  [{'Przeplywy_pieniezne_z_dzialalnosci_operacyjnej': 'CashflowOperatingCashflow'},
                   {'Przeplywy_pieniezne_z_dzialalnosci_inwestycyjnej': 'CashflowInvestingCashflow'},
                   {'Przeplywy_pieniezne_z_dzialalnosci_finansowej': 'CashflowFinancingCashflow'},
                   {'Przeplywy_pieniezne_razem': 'CashflowNetCashflow'},
                   {'Dywidenda': 'CashflowDividend'}],
    'Wskazniki_wartosci_rynkowej':
                  [{'Cena_do_wartosci_ksiegowej': 'CWKCurrent'},
                   {'Cena_do_wartosci_ksiegowej_Grahama': 'CWKGrahamCurrent'},
                   {'Cena_do_przychodow_ze_sprzedazy': 'CPCurrent'},
                   {'Cena_do_zysku': 'CZCurrent'},
                   {'Cena_do_zysku_operacyjnego': 'CZOCurrent'},
                   {'EV_do_przychodow_ze_sprzedazy': 'EVPCurrent'},
                   {'EV_do_EBIT': 'EVEBITCurrent'},
                   {'EV_do_EBITDA': 'EVEBITDACurrent'}],
    'Wskazniki_rentownosci':
                  [{'ROE': 'ROE'},
                   {'ROA': 'ROA'},
                   {'Marza_zysku_operacyjnego': 'OPM'},
                   {'Marza_zysku_netto': 'ROS'},
                   {'Marza_zysku_ze_sprzedazy': 'RS'},
                   {'Marza_zysku_brutto': 'GPM'},
                   {'Marza_zysku_brutto_ze_sprzedazy': 'RBS'},
                   {'Rentownosc_operacyjna_aktywow': 'ROPA'}],
    'Wskazniki_przeplywyw_pienieznych':
                  [{'Udzial_zysku_netto_w_przeplywach_operacyjnych': 'ZNPO'},
                   {'Wskaznik_zrodel_finansowania_inwestycji': 'ZFI'}],
    'Wskazniki_zadluzenia':
                  [{'Zadluzenie_ogolne': 'DTAR'},
                   {'Zadluzenie_kapitalu_wlasnego': 'CG'},
                   {'Zadluzenie_dlugoterminowe': 'LDER'},
                   {'Zadluzenie_srodkow_trwalych': 'PZAT'},
                   {'Wskaznik_ogolnej_sytuacji_finansowej': 'OSF'},
                   {'Zadluzenie_netto_do_EBITDA': 'NetDebtEBITDA'},
                   {'Zadluzenie_finansowe_netto_do_EBITDA': 'DebtFinEBITDA'}],
    'Rating':
                  [{'Rating': ''}]
             }

""" Słownik okresów dla raportów """
period_dict = {
    'Okres': [
        {'Kwartalne': 'Q'},
        {'Roczne': 'Y'},
        {'Skumulowane': 'C'}]
}