import pandas as pd
import requests
from bs4 import BeautifulSoup
from google.cloud import bigquery
    

url = "https://www.bankier.pl/gospodarka/wskazniki-makroekonomiczne/inflacja-rdr-pol"

# creating response object
r = requests.get(url)

soup = BeautifulSoup(r.content, "html.parser")

# searching for all the tables in the website
scraped_data = soup.find_all('table')

# getting the table into 'data' variable and modying to get the table in the
# right format
data = scraped_data[3].get_text().strip().split()
rok = pd.DataFrame(data).iloc[3::3,:].reset_index(drop=True)
kwartal = pd.DataFrame(data).iloc[2::3,:].reset_index(drop=True)
inflacja = pd.DataFrame(data).iloc[4::3,:].reset_index(drop=True)
df = rok.merge(right=kwartal, 
                    how='left', 
                    left_index=True, 
                    right_index=True)
df = df.merge(right=inflacja, 
                            how='left', 
                            left_index=True, 
                            right_index=True)
df.loc[df['0_y'] == 'I', 'o_y'] = '.01.01'
df.loc[df['0_y'] == 'II', 'o_y'] = '.02.01'
df.loc[df['0_y'] == 'III', 'o_y'] = '.03.01'
df.loc[df['0_y'] == 'IV', 'o_y'] = '.04.01'
df.loc[df['0_y'] == 'V', 'o_y'] = '.05.01'
df.loc[df['0_y'] == 'VI', 'o_y'] = '.06.01'
df.loc[df['0_y'] == 'VII', 'o_y'] = '.07.01'
df.loc[df['0_y'] == 'VIII', 'o_y'] = '.08.01'
df.loc[df['0_y'] == 'IX', 'o_y'] = '.09.01'
df.loc[df['0_y'] == 'X', 'o_y'] = '.10.01'
df.loc[df['0_y'] == 'XI', 'o_y'] = '.11.01'
df.loc[df['0_y'] == 'XII', 'o_y'] = '.12.01'

df = df.drop(labels='0_y', axis=1)
df['Date'] = df['0_x'].str.cat(df['o_y'], sep="")
df.drop(labels=['0_x', 'o_y'], axis=1, inplace=True)
df.rename(columns={0: 'Inflacja'}, inplace=True)
df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d').dt.date
print("Koniec programu")