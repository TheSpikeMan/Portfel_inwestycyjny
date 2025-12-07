from bs4 import BeautifulSoup
import pandas as pd
import re


def transform_data(input_data: str, params_dict: dict):
    """

    Parameters
    ----------
    input_data: dane pobrane za pomocą request w formie tekstowej
    params_dict: słownik z charakterystyką raportu

    Returns
    -------

    """
    scraped_data_dict = {}
    soup = BeautifulSoup(input_data, 'html.parser')

    # Nagłówki
    column_names = [row.text for row in soup.find_all('th')]
    expected_length = len(column_names)

    # Przetwarzanie danych
    for row in soup.find_all('tr'):
        # Sprawdzenie czy znajdziemy link
        ticker_link = row.find('a')
        tds = row.find_all('td')
        # Sprawdzenie czy mamy 5 wierszy (expected_lenght)
        if ticker_link and len(tds) == expected_length:
            ticker_tag = ticker_link.text.split()[0]
            tds_texts = [td.text for td in tds]
            # Połączenie danych z nagłówkami
            tds_with_names = dict(zip(column_names, tds_texts))
            scraped_data_dict.update({ticker_tag: tds_with_names})
        else:
            continue
    # Transponuję dane, aby zmienić ich układ
    df = pd.DataFrame(scraped_data_dict).T.reset_index(names=['Ticker'])

    # Zabezpieczam się na wypadek pustego DataFrame
    if not df.empty:
        # Dodanie kolumn z charakterystyką raportu
        df['Typ_raportu'] = params_dict.get('report_type')
        df['Typ_subraportu'] = params_dict.get('sub_report_type')
        df['Miara'] = params_dict.get('measure')
        df['Okres'] = params_dict.get('period')
        df['Rynek'] = params_dict.get('market')

        # Transformacja danych
        df_melted = df.melt(
            id_vars=['Ticker', 'Profil', 'Raport', 'Typ_raportu', 'Typ_subraportu', 'Miara', 'Okres', 'Rynek'],
            var_name='Dane',
            value_name='Wartosc'
        )

        # Usuwam białe znaki, znaki % oraz '+'
        signs_to_exclude_pattern = re.compile(r'[%+ ]')
        df_melted['Wartosc'] = df_melted['Wartosc'].str.replace(signs_to_exclude_pattern, "", regex=True)

        # Zamieniam ',' na '.'
        df_melted['Wartosc'] = df_melted['Wartosc'].str.replace(",", ".", regex=False)

        # Konwersja typu
        df_melted['Wartosc'] = pd.to_numeric(
            df_melted['Wartosc'],
            errors='coerce'
        )

        # Ustawiam kolejność kolumn
        df_final = df_melted[['Ticker', 'Profil', 'Typ_raportu', 'Typ_subraportu', 'Miara', 'Okres', 'Raport', 'Rynek', 'Dane', 'Wartosc']]

        return df_final
    else:
        empty_cols = ['Ticker', 'Profil', 'Typ_raportu', 'Typ_subraportu', 'Miara', 'Okres', 'Raport', 'Rynek', 'Dane', 'Wartosc']
        return pd.DataFrame(columns=empty_cols)
