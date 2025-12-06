from bs4 import BeautifulSoup
import pandas as pd
import re

def transform_data(input_data: str, path: str):
    """

    Parameters
    ----------
    input_data: dane pobrane za pomocą request w formie tekstowej
    path: ścieżka do fragmentu adresu URL, odnoszącego się do raportu

    Returns
    -------

    """
    scraped_data_dict = {}
    keys_to_remove = []
    soup = BeautifulSoup(input_data, 'html.parser')

    # Nagłówki
    pattern = re.compile(rf'.*{path}.*')
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

    df.to_excel("Przychody.xlsx")
