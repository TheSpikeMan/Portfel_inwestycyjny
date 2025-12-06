
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
                  {'Zysk_netto_akcjonariuszy_jednostki_dominujacej': 'IncomeShareholderNetProfit'}]
             }

""" Słownik okresów dla raportów """
period_dict = {
    'Okres': [
        {'Kwartalne': 'Q'},
        {'Roczne': 'Y'},
        {'Skumulowane': 'C'}]
}