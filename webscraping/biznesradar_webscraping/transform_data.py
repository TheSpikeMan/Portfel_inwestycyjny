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
    keys_to_remove = []
    soup = BeautifulSoup(input_data, 'html.parser')

    # Nagłówki
    pattern = re.compile(rf'.*{params_dict.get('path')}.*')
    column_names = [row.text for row in soup.find_all('a', href=pattern)]

    # Przetwarzanie danych
    for row in soup.find_all('tr'):
        ticker_tag = row.find('a').text.split()[0]
        tds = [td.text for td in row.find_all('td')]
        single_dict = {ticker_tag: tds}
        scraped_data_dict.update(single_dict)

    for key, value in scraped_data_dict.items():
        if len(value) != 5:
            keys_to_remove.append(key)
    del scraped_data_dict[keys_to_remove[0]]
    df = pd.DataFrame(scraped_data_dict).T
    df.columns = column_names

    # Dodanie kolumn z charakterystyką raportu
    df.insert(2, 'Typ_raportu', params_dict.get('report_type'))
    df.insert(3, 'Typ_subraportu', params_dict.get('sub_report_type'))
    df.insert(4, 'Miara', params_dict.get('measure'))
    df.insert(5, 'Okres', params_dict.get('period'))
    df.insert(6, 'Rynek', params_dict.get('market'))

    # Transformacja danych
    df_melted = df.melt(
        id_vars=['Profil', 'Raport', 'Typ_raportu', 'Typ_subraportu', 'Miara', 'Okres', 'Rynek'],
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
    df_final = df_melted['Profil, Typ_raportu', 'Typ_subraportu', 'Miara', 'Okres', 'Raport', 'Rynek', 'Dane', 'Wartosc']

    return df_final
