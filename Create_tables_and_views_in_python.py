# -*- coding: utf-8 -*-
"""
Created on Sun Jan  7 19:10:35 2024

@author: grzeg
"""

# 1. Utworzenie datasetu w danym projekcie.

from google.cloud import bigquery

# Definicja parametrów

project = 'projekt-inwestycyjny'
location = 'europe-central2'
nazwa_dataSetu = 'Test2'


# Utworzenie klienta
client = bigquery.Client(
    project = project,
    location = location
    )

# Utworzenie Datasetu
dataset = client.create_dataset(
    bigquery.Dataset(project + "." + nazwa_dataSetu)
    )
# %%

# 2. Utworzenie tabeli w danym projekcie i zbiorze.

from google.cloud import bigquery

# Definicja parametrów

project = 'projekt-inwestycyjny'
location = 'europe-central2'
nazwa_dataSetu = 'Test2'
nazwa_tabeli = "Test_tabeli"

# Utworzenie klienta
client = bigquery.Client(
    project = project,
    location = location
    )

# Utworzenie Tabeli w danym Datasecie
table = client.create_table(
    bigquery.Table(table_ref = (project + "." + nazwa_dataSetu + "." + nazwa_tabeli),
                   schema = (bigquery.SchemaField(name="Ticker", field_type="STRING", mode = "REQUIRED"),
                       bigquery.SchemaField(name="Date", field_type="DATE", mode = "REQUIRED"),
                       bigquery.SchemaField(name="Close", field_type="FLOAT", mode= "REQUIRED")
                       )))

# %%
# 3. Weryfikacja tabel w ramach danego projektu i zbioru

project = 'projekt-inwestycyjny'
location = 'europe-central2'
nazwa_datasetu = 'Dane_instrumentow'

# Utworzenie klienta
client = bigquery.Client(
    project = project,
    location = location)

# Weryfikacja tabel
tables = client.list_tables(
    bigquery.Dataset(project + "." + nazwa_datasetu))
for table in tables:
    print("Nazwa tabeli: " + table.table_id)
    print("Data utworzenia: " +  str(table.created))
    print("Rodzaj tabeli: " +  table.table_type + "\n")


# %%
# 4. Utworzenie widoku w ramach danego projektu i zbioru

project = 'projekt-inwestycyjny'
location = 'europe-central2'
nazwa_datasetu = 'Dane_instrumentow'
nazwa_tabeli = 'Daily'
nazwa_widoku = 'widok'

# Utworzenie klienta
client = bigquery.Client(
    project = project,
    location = location)


query = f"""
    WITH 
    daily AS (SELECT * FROM `{project}.{nazwa_datasetu}.{nazwa_tabeli}`),
    added_averages AS (
      SELECT
        *,
        ROUND(AVG(Close) OVER(PARTITION BY Ticker ORDER BY `Date` ROWS 4 PRECEDING), 2)       AS moving_average_5,
        ROUND(AVG(Close) OVER(PARTITION BY Ticker ORDER BY `Date` ROWS 14 PRECEDING), 2)      AS moving_average_15,
        ROUND(AVG(Close) OVER(PARTITION BY Ticker ORDER BY `Date` ROWS 69 PRECEDING), 2)      AS moving_average_70,
      FROM
        daily
        )
    SELECT * FROM added_averages
    """

# Utworzenie obiektu typu Table

view = bigquery.Table(
    table_ref = (project + "." + nazwa_datasetu + "." + nazwa_widoku))

# Przypisanie do atrybutu view_query wartosci query

view.view_query = query

# Utworzenie widoku

client.create_table(view)

# %%

from google.cloud import bigquery

# 5. Odczyt danych z tabeli i przypisanie do DataFrame

project_id = 'projekt-inwestycyjny'
dataset_id = 'Inflation'
table_id = 'Inflation'
destination_table = f"`{project_id}.{dataset_id}.{table_id}`"

query = f"""
    SELECT *
    FROM {destination_table}
    """

client = bigquery.Client()
query_job = client.query(query)
df = query_job.to_dataframe()
