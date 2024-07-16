# -*- coding: utf-8 -*-
"""
Created on Tue Jan 16 17:41:11 2024

@author: grzeg
"""

from bs4 import BeautifulSoup
import requests
import pandas as pd
import re
import datetime
from google.cloud import bigquery

# Założeniem programu jest import danych giełdowych jakiegokolwiek instrumentu
# finansowego z portalu Google Finance. 

def calculate_close_value(currency, close, present_currencies):
    
    """
    
    Funkcja wyznacza aktualnosc wartosc gieldowa danego instrumentu,
    na podstawie wiedzy, jakiej waluty jest instrument oraz jaka ma wartosc.

    Parameters
    ----------
    currency : STRING - 'EUR' OR 'USD' - waluta
    close : STRING - Wartosc kursu danego instrumentu wraz z walutą
    present_currencies : DATAFRAME - Aktualna wartosc kursu walutowego, dla walut 'EUR' & 'USD'

    Returns
    -------
    close : FLOAT - Wartosc kursu danego instrumentu [PLN]

    """
    
    match currency:
        case 'EUR': print('Dane przedstawione są w EUR.')
        case 'USD': print('Dane przedstawione są w USD.')
        case _ : print("Dane przedstawione są w nieznanej walucie.")
        
    close = close.split('\xa0')[0]
    close = float(close)
    df1 = pd.DataFrame(
        {
        'Currency': [f'{currency}'],
        'Close': [close]
        }
        )
    df1 = df1.merge(present_currencies,
                how = 'inner',
                left_on = 'Currency',
                right_on = 'Currency'
                )
    df1['close_currency'] = df1['Close'] * df1['Currency_close']
    close = df1.loc[0, 'close_currency'].round(decimals=2)
    return close

    
def calculate_present_currencies(project, dataset, table):
    
    """
    Funkcja wyznacza aktualną wartosc kursu walutowego dla dwóch
    głównych walut: dolara oraz euro, na podstawie danych z tabeli GCP, 
    zasilanej codziennie przez Cloud Function.
    
    Parameters
    ----------
    project : STRING - project GCP
    dataset : STRING - dataset w projecie GCP
    table : STRING - tabela w projecie i datasecie GCP

    Returns
    -------
    df : DATAFRAME - tabela z wynikami kursów walutowych dla 'EUR' i 'USD'

    """
    
    client = bigquery.Client()
    query = f"""
    WITH
    all_currencies_data_ordered AS (
      SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY Currency ORDER BY Currency_date DESC) as row_number
      FROM `{project}.{dataset}.{table}`
      WHERE TRUE
    )

    SELECT *
    FROM all_currencies_data_ordered
    WHERE row_number = 1
            

    """
    
    query_job = client.query(query = query)
    present_currencies = query_job.to_dataframe()
    return present_currencies

def webscraping_google_finance(ticker, stock_market = 'WSE', lang = 'pl'):
    
    """
    Funkcja zajmuje się scrapingiem ceny zamknięcia danego instrumentu finansowego

    Parameters
    ----------
    ticker : STRING - nazwa tickera w Google Finance
    stock_market : STRING - nazwa rynku w Google Finance, DEFAULT: 'WSE' -
        polska giełda GPW
    lang : STRING - identyfikator języka, DEFAULT: 'pl' - język polski
    

    Returns
    -------
    close : Wartosc kursu danego instrumentu wraz z walutą

    """
    
    url = f'https://www.google.com/finance/quote/{ticker}:{stock_market}?hl={lang}'
    with requests.get(url=url) as r:
        if r.status_code == 200:
            print('Connection to the site successful.')
        
        soup = BeautifulSoup(r.text, 'html.parser')
        
        # 4. W zależnosci od jednostki dane chcemy przedstawiać w PLN, stąd
        # koniecznosc zbadania jednostki i ewentualnej konwersji
        close = soup.find('div', class_ = 'YMlKec fxKbKc').text.\
            replace(",", ".")
        return close
    

# 1. Zdefiniowanie parametrów wyszukiwania oraz bazy przechowującej 
# kursy walut.

stock_market = 'LON'
ticker = 'IEDY'
lang = 'pl'

project = 'projekt-inwestycyjny'
dataset = 'Waluty'
table = 'Currency'


# 2. Wyznaczenie obecnych kursów walut na podstawie funkcji.

df = calculate_present_currencies(project, dataset, table)

# 3. Pobranie danych dla danego tickera:
    
close = webscraping_google_finance(ticker, stock_market)

# 4. W zależnosci od jednostki dane chcemy przedstawiać w PLN, stąd
# koniecznosc zbadania jednostki i ewentualnej konwersji

pattern_pln = re.compile('.ZŁ.*', re.IGNORECASE)
pattern_eur = re.compile('.EUR.*', re.IGNORECASE)
pattern_usd = re.compile('.$.*', re.IGNORECASE)


if re.findall(pattern_pln, close):
    print('Dane przedstawione są w PLN.')
    
    # 5. Usunięcie znacznika waluty PLN oraz zmiana typu danych.
    close = close.split('\xa0')[0]
    close = float(close)
else:
    if re.findall(pattern_eur, close):
        close = calculate_close_value('EUR', close, df)

    else:
        if re.findall(pattern_usd, close):
            close = calculate_close_value('USD', close, df)


